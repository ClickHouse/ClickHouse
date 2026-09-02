-- Tags: global, no-fasttest

SELECT @@test;
SELECT @@max_allowed_packet FORMAT CSVWithNames;
SELECT @@MAX_ALLOWED_PACKET FORMAT CSVWithNames;
SELECT @@max_allowed_packet, number FROM system.numbers LIMIT 3 FORMAT CSVWithNames;
SELECT @@session.auto_increment_increment FORMAT CSVWithNames;
SELECT @@session.auto_increment_increment AS auto_increment_increment FORMAT CSVWithNames;
SELECT @@Version FORMAT CSVWithNames;
