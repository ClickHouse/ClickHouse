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

SELECT '-- Top-level [: opens a bracket (not a class): a literal [ member after it is kept, not treated as a new bracket';
SELECT ':' SIMILAR TO '[:[]';                 -- Returns: 1
SELECT '[' SIMILAR TO '[:[]';                 -- Returns: 1
SELECT 'a' SIMILAR TO '[:[]';                 -- Returns: 0

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

SELECT '-- Escaped metacharacter inside bracket is a single literal member';
SELECT '-' SIMILAR TO '[\-]';                      -- Returns: 1
SELECT '\\' SIMILAR TO '[\-]';                     -- Returns: 0
SELECT 'a' SIMILAR TO '[\-]';                      -- Returns: 0
SELECT '^' SIMILAR TO '[\^]';                      -- Returns: 1
SELECT '\\' SIMILAR TO '[\^]';                     -- Returns: 0
SELECT '-' SIMILAR TO '[[:digit:]\-]';             -- Returns: 1
SELECT '5' SIMILAR TO '[[:digit:]\-]';             -- Returns: 1
SELECT '\\' SIMILAR TO '[[:digit:]\-]';            -- Returns: 0

SELECT '-- re2 Perl classes are not part of SIMILAR TO: escaped letters inside a bracket are literal';
SELECT 'd' SIMILAR TO '[\d]';                      -- Returns: 1
SELECT '5' SIMILAR TO '[\d]';                      -- Returns: 0
SELECT 'w' SIMILAR TO '[\w]';                      -- Returns: 1
SELECT '_' SIMILAR TO '[\w]';                      -- Returns: 0
SELECT 's' SIMILAR TO '[\s]';                      -- Returns: 1
SELECT ' ' SIMILAR TO '[\s]';                      -- Returns: 0
SELECT 'd' SIMILAR TO '[\da]';                     -- Returns: 1
SELECT 'a' SIMILAR TO '[\da]';                     -- Returns: 1
SELECT '5' SIMILAR TO '[\da]';                     -- Returns: 0

SELECT '-- re2 extension groups like (?...) are not part of SIMILAR TO and are rejected';
SELECT 'A' SIMILAR TO '(?i:a)'; -- { serverError BAD_ARGUMENTS }
SELECT 'a' SIMILAR TO '(?:a)';  -- { serverError BAD_ARGUMENTS }
SELECT 'a' SIMILAR TO '(a)?';                      -- Returns: 1
SELECT ''  SIMILAR TO '(a)?';                      -- Returns: 1

SELECT '-- Escaped excluded metacharacters ^ $ . denote the literal character (same as unescaped)';
SELECT '.'   SIMILAR TO '\.';                      -- Returns: 1
SELECT '\\.' SIMILAR TO '\.';                      -- Returns: 0
SELECT 'a.b' SIMILAR TO 'a\.b';                    -- Returns: 1
SELECT 'axb' SIMILAR TO 'a\.b';                    -- Returns: 0
SELECT '^'   SIMILAR TO '\^';                      -- Returns: 1
SELECT 'a'   SIMILAR TO '\^';                      -- Returns: 0
SELECT '$'   SIMILAR TO '\$';                      -- Returns: 1
SELECT 'a'   SIMILAR TO '\$';                      -- Returns: 0

SELECT '-- Escaped excluded metacharacters in the substring fast path (%...%)';
SELECT '.'    SIMILAR TO '%\.%';                   -- Returns: 1
SELECT 'a.b'  SIMILAR TO '%\.%';                   -- Returns: 1
SELECT 'ab'   SIMILAR TO '%\.%';                   -- Returns: 0
SELECT '^x'   SIMILAR TO '%\^%';                   -- Returns: 1
SELECT 'ab'   SIMILAR TO '%\^%';                   -- Returns: 0
SELECT 'x$y'  SIMILAR TO '%\$%';                   -- Returns: 1
SELECT 'ab'   SIMILAR TO '%\$%';                   -- Returns: 0

SELECT '-- Array quantifiers: SIMILAR TO / NOT SIMILAR TO with SOME / ALL over a pattern array';
SELECT 'abc' SIMILAR TO SOME(['a%', 'z%']);        -- exists: 'abc' SIMILAR TO 'a%'          -> 1
SELECT 'abc' SIMILAR TO ALL(['a%', 'z%']);         -- not all: 'abc' NOT SIMILAR TO 'z%'      -> 0
SELECT 'abc' SIMILAR TO ALL(['a%', '%c']);         -- all: 'abc' matches both                 -> 1
SELECT 'abc' SIMILAR TO SOME(['(a|z)bc', 'x+']);   -- exists: 'abc' matches '(a|z)bc'          -> 1
SELECT 'abc' NOT SIMILAR TO SOME(['z%', 'a%']);    -- exists: 'abc' NOT SIMILAR TO 'z%'        -> 1
SELECT 'abc' NOT SIMILAR TO ALL(['x%', 'y%']);     -- all: 'abc' matches neither pattern       -> 1
SELECT 'abc' NOT SIMILAR TO ALL(['a%', 'z%']);     -- not all: 'abc' SIMILAR TO 'a%'           -> 0
-- Equivalence to the explicit arrayExists / arrayAll lambda form.
SELECT ('abc' SIMILAR TO SOME(['a%', 'z%'])) = arrayExists(_a -> 'abc' SIMILAR TO _a, ['a%', 'z%']);
SELECT ('abc' SIMILAR TO ALL(['a%', '%c'])) = arrayAll(_a -> 'abc' SIMILAR TO _a, ['a%', '%c']);
SELECT ('abc' NOT SIMILAR TO ALL(['x%', 'y%'])) = arrayAll(_a -> 'abc' NOT SIMILAR TO _a, ['x%', 'y%']);

SELECT '-- ESCAPE clause: the escape character makes the next character a literal';
SELECT 'a_b'  SIMILAR TO 'a#_b' ESCAPE '#';        -- Returns: 1 (#_ is a literal _)
SELECT 'axb'  SIMILAR TO 'a#_b' ESCAPE '#';        -- Returns: 0
SELECT 'a%b'  SIMILAR TO 'a#%b' ESCAPE '#';        -- Returns: 1 (#% is a literal %)
SELECT 'aXXb' SIMILAR TO 'a#%b' ESCAPE '#';        -- Returns: 0
SELECT '100%' SIMILAR TO '100!%' ESCAPE '!';       -- Returns: 1 (a different escape char)

SELECT '-- ESCAPE disables a SIMILAR TO metacharacter';
SELECT 'a'    SIMILAR TO 'a|b';                    -- Returns: 1 (unescaped: alternation)
SELECT 'a'    SIMILAR TO 'a#|b' ESCAPE '#';        -- Returns: 0 (escaped: literal 'a|b')
SELECT 'a|b'  SIMILAR TO 'a#|b' ESCAPE '#';        -- Returns: 1
SELECT 'a(b)' SIMILAR TO 'a#(b#)' ESCAPE '#';      -- Returns: 1 (literal parentheses)

SELECT '-- ESCAPE the escape character itself, and escaping a plain character';
SELECT 'a#b'  SIMILAR TO 'a##b' ESCAPE '#';        -- Returns: 1 (## is a literal #)
SELECT 'ab'   SIMILAR TO 'a#b' ESCAPE '#';         -- Returns: 1 (#b is a literal b)

SELECT '-- ESCAPE with backslash behaves like the default escape';
SELECT 'a_b'  SIMILAR TO 'a\\_b' ESCAPE '\\';      -- Returns: 1
SELECT ('a_b' SIMILAR TO 'a\\_b' ESCAPE '\\') = ('a_b' SIMILAR TO 'a\\_b'); -- Returns: 1

SELECT '-- A bare backslash is a literal when a custom escape character is used';
SELECT 'a\\b' SIMILAR TO 'a\\b' ESCAPE '#';        -- Returns: 1

SELECT '-- ESCAPE is inert inside a bracket expression (POSIX/PostgreSQL)';
SELECT '#'   SIMILAR TO '[#a]' ESCAPE '#';         -- Returns: 1 (# is a literal member of the class)
SELECT 'a'   SIMILAR TO '[#a]' ESCAPE '#';         -- Returns: 1
SELECT 'b'   SIMILAR TO '[#a]' ESCAPE '#';         -- Returns: 0
SELECT 'a#b' SIMILAR TO 'a[#]b' ESCAPE '#';        -- Returns: 1 (bracket with literal #, no invalid regexp)

SELECT '-- A bare backslash is a literal member inside a bracket expression under a custom escape';
SELECT '\\'  SIMILAR TO '[\\d]' ESCAPE '#';        -- Returns: 1 (the backslash is a literal member)
SELECT 'd'   SIMILAR TO '[\\d]' ESCAPE '#';        -- Returns: 1
SELECT '5'   SIMILAR TO '[\\d]' ESCAPE '#';        -- Returns: 0 (\d is not the digit class)
SELECT 'a\\c' SIMILAR TO 'a[b\\]c' ESCAPE '#';     -- Returns: 1 (a lone backslash member does not consume the closing bracket)
SELECT 'abc' SIMILAR TO 'a[b\\]c' ESCAPE '#';      -- Returns: 1

SELECT '-- A trailing escape character is an error';
SELECT 'a'   SIMILAR TO 'a#' ESCAPE '#';           -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

SELECT '-- NOT SIMILAR TO with ESCAPE';
SELECT 'a_b'  NOT SIMILAR TO 'a#_b' ESCAPE '#';    -- Returns: 0
SELECT 'axb'  NOT SIMILAR TO 'a#_b' ESCAPE '#';    -- Returns: 1

SELECT '-- The operator form and the 3-argument function form agree';
SELECT ('a_b' SIMILAR TO 'a#_b' ESCAPE '#')     = similarTo('a_b', 'a#_b', '#');    -- Returns: 1
SELECT ('a_b' NOT SIMILAR TO 'a#_b' ESCAPE '#') = notSimilarTo('a_b', 'a#_b', '#'); -- Returns: 1

SELECT '-- ESCAPE round-trips through the formatter';
SELECT formatQuery('SELECT s SIMILAR TO p ESCAPE ''#''');
SELECT formatQuery('SELECT s NOT SIMILAR TO p ESCAPE ''#''');

SELECT '-- ESCAPE must be a single ASCII character';
SELECT 'a' SIMILAR TO 'a' ESCAPE 'ab'; -- { serverError BAD_ARGUMENTS }
