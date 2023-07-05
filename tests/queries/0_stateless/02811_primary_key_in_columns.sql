DROP TABLE IF EXISTS pk_test1;
DROP TABLE IF EXISTS pk_test2;
DROP TABLE IF EXISTS pk_test3;
DROP TABLE IF EXISTS pk_test4;
DROP TABLE IF EXISTS pk_test5;
DROP TABLE IF EXISTS pk_test6;
DROP TABLE IF EXISTS pk_test7;
DROP TABLE IF EXISTS pk_test8;
DROP TABLE IF EXISTS pk_test9;
DROP TABLE IF EXISTS pk_test10;
DROP TABLE IF EXISTS pk_test11;
DROP TABLE IF EXISTS pk_test12;
DROP TABLE IF EXISTS pk_test12;
DROP TABLE IF EXISTS pk_test13;
DROP TABLE IF EXISTS pk_test14;
DROP TABLE IF EXISTS pk_test15;
DROP TABLE IF EXISTS pk_test16;
DROP TABLE IF EXISTS pk_test17;
DROP TABLE IF EXISTS pk_test18;
DROP TABLE IF EXISTS pk_test19;
DROP TABLE IF EXISTS pk_test20;
DROP TABLE IF EXISTS pk_test21;
DROP TABLE IF EXISTS pk_test22;
DROP TABLE IF EXISTS pk_test23;

SET default_table_engine=MergeTree;

CREATE TABLE pk_test1 (String a PRIMARY KEY, String b, String c);
CREATE TABLE pk_test2 (String a PRIMARY KEY, String b PRIMARY KEY, String c);
CREATE TABLE pk_test3 (String a PRIMARY KEY, String b PRIMARY KEY, String c PRIMARY KEY);

CREATE TABLE pk_test4 (String a, String b PRIMARY KEY, String c PRIMARY KEY);
CREATE TABLE pk_test5 (String a, String b PRIMARY KEY, String c);
CREATE TABLE pk_test6 (String a, String b, String c PRIMARY KEY);

CREATE TABLE pk_test7 (String a PRIMARY KEY, String b, String c, PRIMARY KEY (a));
CREATE TABLE pk_test8 (String a PRIMARY KEY, String b PRIMARY KEY, String c, PRIMARY KEY (a));
CREATE TABLE pk_test9 (String a PRIMARY KEY, String b PRIMARY KEY, String c PRIMARY KEY, PRIMARY KEY (a));

CREATE TABLE pk_test10 (String a, String b PRIMARY KEY, String c PRIMARY KEY, PRIMARY KEY (a));
CREATE TABLE pk_test11 (String a, String b PRIMARY KEY, String c, PRIMARY KEY (a));
CREATE TABLE pk_test12 (String a, String b, String c PRIMARY KEY, PRIMARY KEY (a));

CREATE TABLE pk_test12 (String a PRIMARY KEY, String b, String c) PRIMARY KEY (a,b,c);
CREATE TABLE pk_test13 (String a PRIMARY KEY, String b PRIMARY KEY, String c) PRIMARY KEY (a,b,c);
CREATE TABLE pk_test14 (String a PRIMARY KEY, String b PRIMARY KEY, String c PRIMARY KEY) PRIMARY KEY (a,b,c);

CREATE TABLE pk_test15 (String a, String b PRIMARY KEY, String c PRIMARY KEY) PRIMARY KEY (a,b,c);
CREATE TABLE pk_test16 (String a, String b PRIMARY KEY, String c) PRIMARY KEY (a,b,c);
CREATE TABLE pk_test17 (String a, String b, String c PRIMARY KEY) PRIMARY KEY (a,b,c);

CREATE TABLE pk_test18 (String a PRIMARY KEY, String b, String c) ORDER BY (a,b,c);
CREATE TABLE pk_test19 (String a PRIMARY KEY, String b PRIMARY KEY, String c) ORDER BY (a,b,c);
CREATE TABLE pk_test20 (String a PRIMARY KEY, String b PRIMARY KEY, String c PRIMARY KEY) ORDER BY (a,b,c);

CREATE TABLE pk_test21 (String a, String b PRIMARY KEY, String c PRIMARY KEY) ORDER BY (a,b,c);
CREATE TABLE pk_test22 (String a, String b PRIMARY KEY, String c) ORDER BY (a,b,c);
CREATE TABLE pk_test23 (String a, String b, String c PRIMARY KEY) ORDER BY (a,b,c);

DROP TABLE IF EXISTS pk_test1;
DROP TABLE IF EXISTS pk_test2;
DROP TABLE IF EXISTS pk_test3;
DROP TABLE IF EXISTS pk_test4;
DROP TABLE IF EXISTS pk_test5;
DROP TABLE IF EXISTS pk_test6;
DROP TABLE IF EXISTS pk_test7;
DROP TABLE IF EXISTS pk_test8;
DROP TABLE IF EXISTS pk_test9;
DROP TABLE IF EXISTS pk_test10;
DROP TABLE IF EXISTS pk_test11;
DROP TABLE IF EXISTS pk_test12;
DROP TABLE IF EXISTS pk_test12;
DROP TABLE IF EXISTS pk_test13;
DROP TABLE IF EXISTS pk_test14;
DROP TABLE IF EXISTS pk_test15;
DROP TABLE IF EXISTS pk_test16;
DROP TABLE IF EXISTS pk_test17;
DROP TABLE IF EXISTS pk_test18;
DROP TABLE IF EXISTS pk_test19;
DROP TABLE IF EXISTS pk_test20;
DROP TABLE IF EXISTS pk_test21;
DROP TABLE IF EXISTS pk_test22;
DROP TABLE IF EXISTS pk_test23;