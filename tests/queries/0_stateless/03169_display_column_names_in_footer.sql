-- https://github.com/ClickHouse/ClickHouse/issues/65035
SET output_format_pretty_display_footer_column_names=1;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 1000) FORMAT Pretty;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 1000) FORMAT PrettyNoEscapes;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 1000) FORMAT PrettyMonoBlock;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 1000) FORMAT PrettyNoEscapesMonoBlock;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 1000) FORMAT PrettyCompact;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 1000) FORMAT PrettyCompactNoEscapes;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 1000) FORMAT PrettyCompactMonoBlock;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 1000) FORMAT PrettyCompactNoEscapesMonoBlock;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 1000) FORMAT PrettySpace;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 1000) FORMAT PrettySpaceNoEscapes;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 1000) FORMAT PrettySpaceMonoBlock;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 1000) FORMAT PrettySpaceNoEscapesMonoBlock;
