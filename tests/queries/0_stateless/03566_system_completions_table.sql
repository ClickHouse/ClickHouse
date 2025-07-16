CREATE DATABASE IF NOT EXISTS `0003566aaadatabase`;
USE `0003566aaadatabase`;

CREATE TABLE IF NOT EXISTS `0003566aaatable` (
    `0003566aaafoo`     String,
    `0003566aaabar`     UInt16,
    `0003566aaabaz`     UInt128
) ENGINE = Memory;

SELECT DISTINCT lower(word) AS token
FROM system.completions
WHERE startsWith(token, '0003566')
ORDER BY token
LIMIT 5
FORMAT PrettyCompact;
