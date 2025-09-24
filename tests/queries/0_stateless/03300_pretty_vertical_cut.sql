SET output_format_pretty_max_rows = 10;

SELECT number, 'Hello'||number FROM numbers(25) FORMAT Pretty;
SELECT number, 'Hello'||number FROM numbers(25) FORMAT PrettyCompact;
SELECT number, 'Hello'||number FROM numbers(25) FORMAT PrettySpace;

SET output_format_pretty_max_rows = 11;

SELECT number, 'Hello'||number FROM numbers(25) FORMAT Pretty;
SELECT number, 'Hello'||number FROM numbers(25) FORMAT PrettyCompact;
SELECT number, 'Hello'||number FROM numbers(25) FORMAT PrettySpace;
