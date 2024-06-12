-- https://github.com/ClickHouse/ClickHouse/issues/65035
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 49) FORMAT Pretty;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 10) FORMAT Pretty SETTINGS output_format_pretty_display_footer_column_names_min_rows=9;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT Pretty SETTINGS output_format_pretty_display_footer_column_names=0;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT Pretty;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyNoEscapes;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyMonoBlock;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyNoEscapesMonoBlock;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyNoEscapesMonoBlock;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyCompact SETTINGS output_format_pretty_display_footer_column_names=0;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyCompact;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyCompactNoEscapes;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyCompactMonoBlock;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyCompactNoEscapesMonoBlock;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettySpace SETTINGS output_format_pretty_display_footer_column_names=0;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettySpace;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettySpaceNoEscapes;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettySpaceMonoBlock;
SELECT *, toTypeName(*), mod(*,2) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettySpaceNoEscapesMonoBlock;
