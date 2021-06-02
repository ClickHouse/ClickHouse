DROP DATABASE IF EXISTS sqlite_database;

CREATE DATABASE sqlite_database
ENGINE = SQLite('../../ClickHouse/tests/queries/0_stateless/test.sqlite3');

SELECT * FROM sqlite_database.`Some table`;

SELECT `some field` FROM sqlite_database.`Some table`;

SELECT `string field` FROM sqlite_database.`Some table`;

SELECT * FROM sqlite_database.`Empty table`;

SELECT field2 FROM sqlite_database.`Empty table`;
