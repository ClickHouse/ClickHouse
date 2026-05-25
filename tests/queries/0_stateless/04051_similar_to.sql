-- tests of SIMILAR TO pattern search

SELECT '-- +: one or more';
SELECT 'hello'   SIMILAR TO 'hel+o';           -- Returns: 1
SELECT 'heo'     SIMILAR TO 'hel+o';           -- Returns: 0

SELECT '-- *: zero or more';
SELECT 'heo'     SIMILAR TO 'hel*o';           -- Returns: 1
SELECT 'hello'   SIMILAR TO 'hel*o';           -- Returns: 1

SELECT '-- ?: zero or one';
SELECT 'helo'    SIMILAR TO 'hel?o';           -- Returns: 1
SELECT 'hello'   SIMILAR TO 'hel?o';           -- Returns: 0

SELECT '-- _: any single char';
SELECT 'hello'   SIMILAR TO 'hel_o';           -- Returns: 1

SELECT '-- %: zero or more chars (substring match)';
SELECT 'hello'   SIMILAR TO '%(el|er)%';       -- Returns: 1
SELECT 'herring' SIMILAR TO '%(el|er)%';       -- Returns: 1

SELECT '-- Anchored by default (matches full string)';
SELECT 'hello world' SIMILAR TO 'hello';       -- Returns: 0
SELECT 'hello world' SIMILAR TO '%hello%';     -- Returns: 1

SELECT '-- []: character class';
SELECT 'hello'   SIMILAR TO 'he[l]+o';         -- Returns: 1
SELECT 'hello'   SIMILAR TO 'he[r]+o';         -- Returns: 0
SELECT 'test123' SIMILAR TO '[a-z]+[0-9]+';    -- Returns: 1

SELECT '-- [^]: negated bracket expression';
SELECT 'hello'   SIMILAR TO 'hell[^aeiou]';    -- Returns: 0
SELECT 'hellx'   SIMILAR TO 'hell[^aeiou]';    -- Returns: 1

SELECT '-- POSIX character classes';
SELECT '123'     SIMILAR TO '[[:digit:]]+';    -- Returns: 1
SELECT 'hello'   SIMILAR TO '[[:alpha:]]+';    -- Returns: 1
SELECT 'hello'   SIMILAR TO '[[:lower:]]+';    -- Returns: 1
SELECT 'Hello'   SIMILAR TO '[[:lower:]]+';    -- Returns: 0

SELECT '-- Repeated count';
SELECT 'abc123' SIMILAR TO '%[0-9]{3}';        -- Returns: 1
SELECT 'abc12'  SIMILAR TO '%[0-9]{3}';        -- Returns: 0
SELECT '123'    SIMILAR TO '[0-9]{2,4}';       -- Returns: 1
SELECT '12345'  SIMILAR TO '[0-9]{2,4}';       -- Returns: 0

SELECT '-- Escaping metacharacters';
SELECT 'a+b'     SIMILAR TO 'a\+b';           -- Returns: 1
SELECT 'aab'     SIMILAR TO 'a\+b';           -- Returns: 0
SELECT 'a(b)'    SIMILAR TO 'a\(b\)';         -- Returns: 1
SELECT 'a[b]'    SIMILAR TO 'a\[b\]';         -- Returns: 1

SELECT '-- Escaping inside brackets';
SELECT '%'       SIMILAR TO '[\%]';           -- Returns: 1
SELECT '_'       SIMILAR TO '[\_]';           -- Returns: 1
SELECT '|'       SIMILAR TO '[\|]';           -- Returns: 1
SELECT '+'       SIMILAR TO '[\+]';           -- Returns: 1
SELECT '('       SIMILAR TO '[\(]';           -- Returns: 1
SELECT '\\'      SIMILAR TO '[\\\\]';         -- Returns: 1

SELECT '-- % and _ are literal inside bracket expressions';
SELECT '%'       SIMILAR TO '[%_]';            -- Returns: 1
SELECT 'a'       SIMILAR TO '[%_]';            -- Returns: 0

SELECT '-- Empty string';
SELECT ''        SIMILAR TO '';                -- Returns: 1
SELECT ''        SIMILAR TO '%';              -- Returns: 1
SELECT ''        SIMILAR TO '_';              -- Returns: 0

SELECT '-- Nested grouping';
SELECT 'ac'      SIMILAR TO '((a|b)c|d)';     -- Returns: 1
SELECT 'd'       SIMILAR TO '((a|b)c|d)';     -- Returns: 1
SELECT 'ab'      SIMILAR TO '((a|b)c|d)';     -- Returns: 0

SELECT '-- Many patterns in one';
SELECT 'AB5 xfoo!+doneZZ' SIMILAR TO '[[:upper:]]{2,3}[0-9][[:space:]]_((foo|bar)_\+[^0-9]?[[:lower:]]*)%[[:upper:]]+';  -- Returns: 1

SELECT '-- Function syntax';
SELECT similarTo('hello', 'hel+o');           -- Returns: 1

SELECT '-- Regex but non-SIMILAR-TO metachars: ^$.';
SELECT '^hello' SIMILAR TO '^hello';          -- Returns: 1
SELECT 'hello$' SIMILAR TO 'hello$';          -- Returns: 1
SELECT 'h.llo'  SIMILAR TO 'h.llo';           -- Returns: 1

SELECT '-- NOT SIMILAR TO';
SELECT 'hello' NOT SIMILAR TO 'hel+o';        -- Returns: 0
SELECT 'hello' NOT SIMILAR TO 'he+lo';        -- Returns: 1

SELECT '-- Character class within bracket expression';
SELECT '_' SIMILAR TO '[[:digit:]_]';         -- Returns: 1
SELECT 'a' SIMILAR TO '[[:digit:]_]';         -- Returns: 0
SELECT '%' SIMILAR TO '[[:digit:]%]';         -- Returns: 1
SELECT '_' SIMILAR TO '[_[:digit:]]';         -- Returns: 1
SELECT '%' SIMILAR TO '[%[:digit:]]';         -- Returns: 1

SELECT '-- Bracket expression ending with colon';
SELECT 'a' SIMILAR TO '[a:]';                 -- Returns: 1
SELECT ':' SIMILAR TO '[a:]';                 -- Returns: 1
SELECT 'a' SIMILAR TO '[a:]_';                -- Returns: 0
SELECT 'ab' SIMILAR TO '[a:]_';               -- Returns: 1

SELECT '-- Bracket expression starting with colon';
SELECT ':' SIMILAR TO '[:a]';                 -- Returns: 1
SELECT 'b' SIMILAR TO '[:a]_';                -- Returns: 0
SELECT 'ab' SIMILAR TO '[:a]_';               -- Returns: 1

SELECT '-- Bracket expression containing metacharacter';
SELECT '_' SIMILAR TO '[:_a]';                -- Returns: 1
SELECT '_' SIMILAR TO '[_a:]';                -- Returns: 1
SELECT '.' SIMILAR TO '[:_a]';                -- Returns: 0
SELECT '.' SIMILAR TO '[_a:]';                -- Returns: 0
SELECT '.' SIMILAR TO '[:.a]';                -- Returns: 1

SELECT '-- Multiple classes';
SELECT 'f' SIMILAR TO '[[:alpha:][:digit:]]'; -- Returns: 1
SELECT '1' SIMILAR TO '[[:alpha:][:digit:]]'; -- Returns: 1
SELECT '_' SIMILAR TO '[[:alpha:][:digit:]]'; -- Returns: 0
SELECT '^' SIMILAR TO '[[:alpha:][:digit:]]'; -- Returns: 0

SELECT '-- Metacharacter before/after class in bracket expression';
SELECT '_' SIMILAR TO '[_[:digit:]]';              -- Returns: 1
SELECT '5' SIMILAR TO '[_[:digit:]]';              -- Returns: 1
SELECT 'a' SIMILAR TO '[_[:digit:]]';              -- Returns: 0

SELECT '%' SIMILAR TO '[[:digit:]%]';              -- Returns: 1
SELECT '5' SIMILAR TO '[[:digit:]%]';              -- Returns: 1
SELECT 'a' SIMILAR TO '[[:digit:]%]';              -- Returns: 0

SELECT '-- Two classes with literal between';
SELECT '_' SIMILAR TO '[[:digit:]_[:alpha:]]';     -- Returns: 1
SELECT 'a' SIMILAR TO '[[:digit:]_[:alpha:]]';     -- Returns: 1
SELECT '5' SIMILAR TO '[[:digit:]_[:alpha:]]';     -- Returns: 1
SELECT '!' SIMILAR TO '[[:digit:]_[:alpha:]]';     -- Returns: 0

SELECT '-- Top-level alternation must match the whole string';
SELECT 'abc'    SIMILAR TO 'abc|def';              -- Returns: 1
SELECT 'def'    SIMILAR TO 'abc|def';              -- Returns: 1
SELECT 'abcdef' SIMILAR TO 'abc|def';              -- Returns: 0
SELECT 'xabc'   SIMILAR TO 'abc|def';              -- Returns: 0
SELECT 'defx'   SIMILAR TO 'abc|def';              -- Returns: 0
SELECT 'a'      SIMILAR TO 'a|b|c';                -- Returns: 1
SELECT 'd'      SIMILAR TO 'a|b|c';                -- Returns: 0
SELECT 'ab'     SIMILAR TO 'a|b';                  -- Returns: 0

SELECT '-- Leading ] inside bracket is a literal (POSIX rule)';
SELECT ']' SIMILAR TO '[]_%]';                     -- Returns: 1
SELECT '_' SIMILAR TO '[]_%]';                     -- Returns: 1
SELECT '%' SIMILAR TO '[]_%]';                     -- Returns: 1
SELECT 'a' SIMILAR TO '[]_%]';                     -- Returns: 0
SELECT ']' SIMILAR TO '[^]_%]';                    -- Returns: 0
SELECT 'a' SIMILAR TO '[^]_%]';                    -- Returns: 1

SELECT '-- Literal [ inside bracket (no spurious lookahead)';
SELECT '['  SIMILAR TO '[[]';                      -- Returns: 1
SELECT 'a'  SIMILAR TO '[[]';                      -- Returns: 0
SELECT '['  SIMILAR TO '[^[]';                     -- Returns: 0
SELECT 'a'  SIMILAR TO '[^[]';                     -- Returns: 1
SELECT '[a' SIMILAR TO '[[a]_';                    -- Returns: 1
SELECT 'aa' SIMILAR TO '[[a]_';                    -- Returns: 1
SELECT 'xb' SIMILAR TO '[[a]_';                    -- Returns: 0
