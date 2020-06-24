SELECT @@test; -- empty string
SELECT @@max_allowed_packet;
SELECT @@MAX_ALLOWED_PACKET;
SELECT @@max_allowed_packet, number FROM system.numbers LIMIT 3;
SHOW VARIABLES LIKE 'lower_case_table_names';
