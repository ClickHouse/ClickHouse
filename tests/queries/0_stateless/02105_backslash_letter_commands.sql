-- Tags: no-parallel

DROP DATABASE IF EXISTS zbackslash_letter_commands;
CREATE DATABASE zbackslash_letter_commands;
CREATE TABLE zbackslash_letter_commands.A (A UInt8) ENGINE = TinyLog;
CREATE TABLE zbackslash_letter_commands.B (A UInt8) ENGINE = TinyLog;

-- SHOW DATABASES;
-- USE zbackslash_letter_commands;
-- SHOW TABLES;

SELECT trim('-- DATABASES --');
\l ;

SELECT trim('-- TABLES --');
\c zbackslash_letter_commands;
\d;


DROP DATABASE zbackslash_letter_commands;
