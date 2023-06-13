-- Tags: no-parallel
DROP TABLE IF EXISTS t_2710_show_table;

CREATE TABLE t_2710_show_table(n1 UInt32, s String) engine=Log;
SHOW TABLE t_2710_show_table;
SHOW CREATE TABLE t_2710_show_table;
SHOW CREATE t_2710_show_table;

DROP TABLE t_2710_show_table;

DROP DATABASE IF EXISTS t_2710_db;
CREATE DATABASE t_2710_db engine=Atomic;
SHOW DATABASE t_2710_db;
SHOW CREATE DATABASE t_2710_db;

DROP DATABASE t_2710_db;
