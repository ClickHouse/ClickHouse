-- Tuple element names long enough that a JSON key is matched in 8-byte steps, including names
-- that are equal in the first 8 bytes and names that are equal in every byte but the last ones.

SELECT JSONExtract('{"quantity":1,"discount":2}', 'Tuple(quantity UInt8, discount UInt8)');
SELECT JSONExtract('{"discount":2,"quantity":1}', 'Tuple(quantity UInt8, discount UInt8)');
SELECT JSONExtract('{"customer_note":"x"}', 'Tuple(customer_name String, customer_note String)');
SELECT JSONExtract('{"shippers_address":"b"}', 'Tuple(shipping_address String, shippers_address String)');
SELECT JSONExtract('{"shipping_address":"a","shippers_address":"b"}', 'Tuple(shipping_address String, shippers_address String)');
SELECT JSONExtract('{"customer_addr":"z","customer_name":"y"}', 'Tuple(customer_name String, customer_note String)');
SELECT JSONExtract(materialize('{"customer_note":"x","customer_name":"y"}'), 'Tuple(customer_name String, customer_note String)') FROM numbers(3);
