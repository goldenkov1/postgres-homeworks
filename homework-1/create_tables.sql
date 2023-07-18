-- SQL-команды для создания таблиц
CREATE TABLE customers_data
(
    customer_id	varchar(50) PRIMARY KEY NOT NULL,
    company_name varchar(100),
    contact_name varchar(100)
);

CREATE TABLE employees_data
(
    employee_id int PRIMARY KEY NOT NULL,
    first_name varchar(50),
    last_name varchar(50),
	title varchar(100),
	birth_date date,
	notes text
);

CREATE TABLE orders_data
(
    order_id int PRIMARY KEY NOT NULL,
    customer_id varchar REFERENCES customers_data(customer_id),
    employee_id integer REFERENCES employees_data(employee_id),
	order_date date,
	ship_city varchar(100)
);