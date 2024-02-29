SELECT 1_000_000 as a FORMAT Pretty;
SELECT 1_000_000 as a FORMAT PrettyNoEscapes;
SELECT 1_000_000 as a FORMAT PrettyMonoBlock;
SELECT 1_000_000 as a FORMAT PrettyNoEscapesMonoBlock;
SELECT 1_000_000 as a FORMAT PrettyCompact;
SELECT 1_000_000 as a FORMAT PrettyCompactNoEscapes;
SELECT 1_000_000 as a FORMAT PrettyCompactMonoBlock;
SELECT 1_000_000 as a FORMAT PrettyCompactNoEscapesMonoBlock;
SELECT 1_000_000 as a FORMAT PrettySpace;
SELECT 1_000_000 as a FORMAT PrettySpaceNoEscapes;
SELECT 1_000_000 as a FORMAT PrettySpaceMonoBlock;
SELECT 1_000_000 as a FORMAT PrettySpaceNoEscapesMonoBlock;


SELECT 1_000_000 as a SETTINGS output_format_pretty_single_large_number_tip_threshold = 1000 FORMAT Pretty;
SELECT 1_000_000 as a SETTINGS output_format_pretty_single_large_number_tip_threshold = 1000 FORMAT PrettyNoEscapes;
SELECT 1_000_000 as a SETTINGS output_format_pretty_single_large_number_tip_threshold = 1000 FORMAT PrettyMonoBlock;
SELECT 1_000_000 as a SETTINGS output_format_pretty_single_large_number_tip_threshold = 1000 FORMAT PrettyNoEscapesMonoBlock;
SELECT 1_000_000 as a SETTINGS output_format_pretty_single_large_number_tip_threshold = 1000 FORMAT PrettyCompact;
SELECT 1_000_000 as a SETTINGS output_format_pretty_single_large_number_tip_threshold = 1000 FORMAT PrettyCompactNoEscapes;
SELECT 1_000_000 as a SETTINGS output_format_pretty_single_large_number_tip_threshold = 1000 FORMAT PrettyCompactMonoBlock;
SELECT 1_000_000 as a SETTINGS output_format_pretty_single_large_number_tip_threshold = 1000 FORMAT PrettyCompactNoEscapesMonoBlock;
SELECT 1_000_000 as a SETTINGS output_format_pretty_single_large_number_tip_threshold = 1000 FORMAT PrettySpace;
SELECT 1_000_000 as a SETTINGS output_format_pretty_single_large_number_tip_threshold = 1000 FORMAT PrettySpaceNoEscapes;
SELECT 1_000_000 as a SETTINGS output_format_pretty_single_large_number_tip_threshold = 1000 FORMAT PrettySpaceMonoBlock;
SELECT 1_000_000 as a SETTINGS output_format_pretty_single_large_number_tip_threshold = 1000 FORMAT PrettySpaceNoEscapesMonoBlock;

SELECT 1_000_001 as a FORMAT Pretty;
SELECT 1_000_001 as a FORMAT PrettyNoEscapes;
SELECT 1_000_001 as a FORMAT PrettyMonoBlock;
SELECT 1_000_001 as a FORMAT PrettyNoEscapesMonoBlock;
SELECT 1_000_001 as a FORMAT PrettyCompact;
SELECT 1_000_001 as a FORMAT PrettyCompactNoEscapes;
SELECT 1_000_001 as a FORMAT PrettyCompactMonoBlock;
SELECT 1_000_001 as a FORMAT PrettyCompactNoEscapesMonoBlock;
SELECT 1_000_001 as a FORMAT PrettySpace;
SELECT 1_000_001 as a FORMAT PrettySpaceNoEscapes;
SELECT 1_000_001 as a FORMAT PrettySpaceMonoBlock;
SELECT 1_000_001 as a FORMAT PrettySpaceNoEscapesMonoBlock;

SELECT 1_000_000_000 as a FORMAT Pretty;
SELECT 1_000_000_000 as a FORMAT PrettyNoEscapes;
SELECT 1_000_000_000 as a FORMAT PrettyMonoBlock;
SELECT 1_000_000_000 as a FORMAT PrettyNoEscapesMonoBlock;
SELECT 1_000_000_000 as a FORMAT PrettyCompact;
SELECT 1_000_000_000 as a FORMAT PrettyCompactNoEscapes;
SELECT 1_000_000_000 as a FORMAT PrettyCompactMonoBlock;
SELECT 1_000_000_000 as a FORMAT PrettyCompactNoEscapesMonoBlock;
SELECT 1_000_000_000 as a FORMAT PrettySpace;
SELECT 1_000_000_000 as a FORMAT PrettySpaceNoEscapes;
SELECT 1_000_000_000 as a FORMAT PrettySpaceMonoBlock;
SELECT 1_000_000_000 as a FORMAT PrettySpaceNoEscapesMonoBlock;

SELECT 1_000_000_000 as a, 1_000_000_000 as b FORMAT Pretty;
SELECT 1_000_000_000 as a, 1_000_000_000 as b FORMAT PrettyNoEscapes;
SELECT 1_000_000_000 as a, 1_000_000_000 as b FORMAT PrettyMonoBlock;
SELECT 1_000_000_000 as a, 1_000_000_000 as b FORMAT PrettyNoEscapesMonoBlock;
SELECT 1_000_000_000 as a, 1_000_000_000 as b FORMAT PrettyCompact;
SELECT 1_000_000_000 as a, 1_000_000_000 as b FORMAT PrettyCompactNoEscapes;
SELECT 1_000_000_000 as a, 1_000_000_000 as b FORMAT PrettyCompactMonoBlock;
SELECT 1_000_000_000 as a, 1_000_000_000 as b FORMAT PrettyCompactNoEscapesMonoBlock;
SELECT 1_000_000_000 as a, 1_000_000_000 as b FORMAT PrettySpace;
SELECT 1_000_000_000 as a, 1_000_000_000 as b FORMAT PrettySpaceNoEscapes;
SELECT 1_000_000_000 as a, 1_000_000_000 as b FORMAT PrettySpaceMonoBlock;
SELECT 1_000_000_000 as a, 1_000_000_000 as b FORMAT PrettySpaceNoEscapesMonoBlock;

SELECT 1_000_000_000 as a FROM system.numbers LIMIT 2 FORMAT Pretty;
SELECT 1_000_000_000 as a FROM system.numbers LIMIT 2 FORMAT PrettyNoEscapes;
SELECT 1_000_000_000 as a FROM system.numbers LIMIT 2 FORMAT PrettyMonoBlock;
SELECT 1_000_000_000 as a FROM system.numbers LIMIT 2 FORMAT PrettyNoEscapesMonoBlock;
SELECT 1_000_000_000 as a FROM system.numbers LIMIT 2 FORMAT PrettyCompact;
SELECT 1_000_000_000 as a FROM system.numbers LIMIT 2 FORMAT PrettyCompactNoEscapes;
SELECT 1_000_000_000 as a FROM system.numbers LIMIT 2 FORMAT PrettyCompactMonoBlock;
SELECT 1_000_000_000 as a FROM system.numbers LIMIT 2 FORMAT PrettyCompactNoEscapesMonoBlock;
SELECT 1_000_000_000 as a FROM system.numbers LIMIT 2 FORMAT PrettySpace;
SELECT 1_000_000_000 as a FROM system.numbers LIMIT 2 FORMAT PrettySpaceNoEscapes;
SELECT 1_000_000_000 as a FROM system.numbers LIMIT 2 FORMAT PrettySpaceMonoBlock;
SELECT 1_000_000_000 as a FROM system.numbers LIMIT 2 FORMAT PrettySpaceNoEscapesMonoBlock;

