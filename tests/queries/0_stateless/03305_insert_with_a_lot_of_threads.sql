SET max_rows_to_read=0;
CREATE TABLE testing_a_lot_of_threads (a UInt64, b String) engine=MergeTree ORDER BY ();
INSERT INTO testing_a_lot_of_threads SELECT number, toString(number) FROM system.numbers LIMIT 1000000000 SETTINGS max_insert_threads=10000;
SELECT count(*) FROM testing_a_lot_of_threads;
