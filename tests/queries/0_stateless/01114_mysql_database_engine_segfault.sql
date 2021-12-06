-- Tags: no-parallel, no-fasttest

DROP DATABASE IF EXISTS conv_main;
CREATE DATABASE conv_main ENGINE = MySQL('127.0.0.1:3456', conv_main, 'metrika', 'password'); -- { serverError 501 }
