CREATE TABLE check_query_tiny_log (UInt32 N, String S) Engine = TinyLog;

INSERT INTO check_query_tiny_log VALUES (1, "A"), (2, "B"), (3, "C")

DROP TABLE check_query_tiny_log;
