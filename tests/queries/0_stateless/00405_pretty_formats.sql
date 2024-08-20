SET output_format_pretty_display_footer_column_names=0;
SET output_format_pretty_color = 1;

SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT Pretty;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettyCompact;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettySpace;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettyCompactMonoBlock;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettyNoEscapes;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettyCompactNoEscapes;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettySpaceNoEscapes;

SET output_format_pretty_max_rows = 6;

SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT Pretty;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettyCompact;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettySpace;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettyCompactMonoBlock;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettyNoEscapes;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettyCompactNoEscapes;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettySpaceNoEscapes;


SET output_format_pretty_grid_charset = 'ASCII';

SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT Pretty;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettyCompact;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettySpace;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettyCompactMonoBlock;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettyNoEscapes;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettyCompactNoEscapes;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 10 SETTINGS max_block_size = 5 FORMAT PrettySpaceNoEscapes;
