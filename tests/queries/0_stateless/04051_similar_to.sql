-- tests of SIMILAR TO pattern search

SELECT '-- +: one or more';
SELECT 'hello'   SIMILAR TO 'hel+o';           -- Returns: 1 (+ = one or more)
SELECT 'helo'    SIMILAR TO 'hel+o';           -- Returns: 1
SELECT 'heo'     SIMILAR TO 'hel+o';           -- Returns: 0 (+ requires at least one l)

SELECT '-- Substring with pattern';
SELECT 'hello'   SIMILAR TO '%(el|er)%';       -- Returns: 1
SELECT 'herring' SIMILAR TO '%(el|er)%';       -- Returns: 1

SELECT '-- Substring without pattern';
SELECT 'hello'   SIMILAR TO '%el%';            -- Returns: 1
SELECT 'hello'   SIMILAR TO '%er%';            -- Returns: 0

SELECT '-- _: any single char';
SELECT 'hello'   SIMILAR TO 'hel_o';           -- Returns: 1 (_ = any single char)

SELECT '-- []: character class';
SELECT 'hello'   SIMILAR TO 'he[l]+o';         -- Returns: 1
SELECT 'hello'   SIMILAR TO 'he[r]+o';         -- Returns: 0
SELECT 'test123' SIMILAR TO '[a-z]+[0-9]+';    -- Returns: 1
SELECT '123'     SIMILAR TO '[[:digit:]]+';    -- Returns: 1

SELECT '-- Anchored by default (matches full string)';
SELECT 'hello world' SIMILAR TO 'hello';       -- Returns: 0 (not full match)
SELECT 'hello world' SIMILAR TO '%hello%';     -- Returns: 1

SELECT '-- Escaping rule';
SELECT '%%'     SIMILAR TO '\%[\%]+';          -- Returns: 1
SELECT '%_'     SIMILAR TO '(\%\_|\_\%)';      -- Returns: 1

SELECT '-- Repeated count';
SELECT 'abc123' SIMILAR TO '%[0-9]{3}';        -- Returns: 1
SELECT 'abc12'  SIMILAR TO '%[0-9]{3}';        -- Returns: 0
SELECT 'abcd'   SIMILAR TO '[a-z]{2,}';        -- Returns: 1
SELECT 'a'      SIMILAR TO '[a-z]{2,}';        -- Returns: 0
SELECT '123'    SIMILAR TO '[0-9]{2,4}';       -- Returns: 1
SELECT '12345'  SIMILAR TO '[0-9]{2,4}';       -- Returns: 0

SELECT '-- All patterns in one';
-- Match: 2-3 uppercase letters, then a digit, then optional whitespace,
-- then one or more alphanumeric chars, then either "foo" or "bar",
-- then any single char, then zero or more of anything
SELECT 'AB5 xyzfoo!done' SIMILAR TO '[[:upper:]]{2,3}[0-9][[:space:]]_[[:alnum:]]+(foo|bar)_%';
