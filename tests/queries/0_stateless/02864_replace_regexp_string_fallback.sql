-- Tests functions replaceRegexpAll and replaceRegexpOne with trivial patterns. These trigger internally a fallback to simple string replacement.

-- _materialize_ because the shortcut is only implemented for non-const haystack + const needle + const replacement strings

SELECT 'Hello' AS haystack, 'l' AS needle, 'x' AS replacement, replaceRegexpOne(materialize(haystack), needle, replacement), replaceRegexpAll(materialize(haystack), needle, replacement);

-- negative tests

-- Even if the fallback is used, invalid substitutions must throw an exception.
SELECT 'Hello' AS haystack, 'l' AS needle, '\\1' AS replacement, replaceRegexpOne(materialize(haystack), needle, replacement); -- { serverError BAD_ARGUMENTS }
SELECT 'Hello' AS haystack, 'l' AS needle, '\\1' AS replacement, replaceRegexpAll(materialize(haystack), needle, replacement); -- { serverError BAD_ARGUMENTS }
