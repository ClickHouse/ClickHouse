SET output_format_pretty_color=1;
SET output_format_pretty_row_numbers=0;
SET output_format_pretty_display_footer_column_names=0;
SET output_format_pretty_squash_consecutive_ms = 0;

SELECT * FROM numbers(10) FORMAT Pretty;
SELECT * FROM numbers(10) FORMAT PrettyCompact;
SELECT * FROM numbers(10) FORMAT PrettyCompactMonoBlock;
SELECT * FROM numbers(10) FORMAT PrettyNoEscapes;
SELECT * FROM numbers(10) FORMAT PrettyCompactNoEscapes;
SELECT * FROM numbers(10) FORMAT PrettySpaceNoEscapes;
SELECT * FROM numbers(10) FORMAT PrettySpace;
SET output_format_pretty_row_numbers=1;
SELECT * FROM numbers(10) FORMAT Pretty;
SELECT * FROM numbers(10) FORMAT PrettyCompact;
SELECT * FROM numbers(10) FORMAT PrettyCompactMonoBlock;
SELECT * FROM numbers(10) FORMAT PrettyNoEscapes;
SELECT * FROM numbers(10) FORMAT PrettyCompactNoEscapes;
SELECT * FROM numbers(10) FORMAT PrettySpaceNoEscapes;
SELECT * FROM numbers(10) FORMAT PrettySpace;

SET max_block_size=1;

SELECT * FROM (SELECT 1 AS a UNION ALL SELECT 2 as a) ORDER BY a FORMAT Pretty;
SELECT * FROM (SELECT 1 AS a UNION ALL SELECT 2 as a) ORDER BY a FORMAT PrettyCompact;
SELECT * FROM (SELECT 1 AS a UNION ALL SELECT 2 as a) ORDER BY a FORMAT PrettyCompactMonoBlock;
SELECT * FROM (SELECT 1 AS a UNION ALL SELECT 2 as a) ORDER BY a FORMAT PrettyNoEscapes;
SELECT * FROM (SELECT 1 AS a UNION ALL SELECT 2 as a) ORDER BY a FORMAT PrettyCompactNoEscapes;
SELECT * FROM (SELECT 1 AS a UNION ALL SELECT 2 as a) ORDER BY a FORMAT PrettySpace;
SELECT * FROM (SELECT 1 AS a UNION ALL SELECT 2 as a) ORDER BY a FORMAT PrettySpaceNoEscapes;

SELECT * FROM numbers(10) ORDER BY number FORMAT Pretty;
SELECT * FROM numbers(10) ORDER BY number FORMAT PrettyCompact;
SELECT * FROM numbers(10) ORDER BY number FORMAT PrettyCompactMonoBlock;
SELECT * FROM numbers(10) ORDER BY number FORMAT PrettyNoEscapes;
SELECT * FROM numbers(10) ORDER BY number FORMAT PrettyCompactNoEscapes;
SELECT * FROM numbers(10) ORDER BY number FORMAT PrettySpace;
SELECT * FROM numbers(10) ORDER BY number FORMAT PrettySpaceNoEscapes;
