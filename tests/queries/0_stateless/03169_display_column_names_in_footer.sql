-- https://github.com/ClickHouse/ClickHouse/issues/65035
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 49) FORMAT Pretty;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 10) FORMAT Pretty SETTINGS output_format_pretty_display_footer_column_names_min_rows=9;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT Pretty SETTINGS output_format_pretty_display_footer_column_names=0;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT Pretty;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyNoEscapes;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyMonoBlock;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyNoEscapesMonoBlock;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyNoEscapesMonoBlock;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyCompact SETTINGS output_format_pretty_display_footer_column_names=0;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyCompact;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyCompactNoEscapes;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyCompactMonoBlock;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettyCompactNoEscapesMonoBlock;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettySpace SETTINGS output_format_pretty_display_footer_column_names=0;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettySpace;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettySpaceNoEscapes;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettySpaceMonoBlock;
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 100) FORMAT PrettySpaceNoEscapesMonoBlock;
