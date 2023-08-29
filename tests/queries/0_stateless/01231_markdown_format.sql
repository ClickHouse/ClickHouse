DROP TABLE IF EXISTS markdown;
CREATE TABLE markdown (id UInt32, name String, array Array(Int32), nullable Nullable(String), low_cardinality LowCardinality(String), decimal Decimal32(6)) ENGINE = Memory;
INSERT INTO markdown VALUES (1, 'name1', [1,2,3], 'Some long string', 'name1', 1.11), (2, 'name2', [4,5,60000], Null, 'Another long string', 222.222222), (30000, 'One more long string', [7,8,9], 'name3', 'name3', 3.33);

SELECT * FROM markdown FORMAT Markdown;
DROP TABLE IF EXISTS markdown;


DROP TABLE IF EXISTS markdown;
SET output_format_markdown_escape_special_characters = true;
SELECT '!' as a, '#' as b, '$' as c, '%' as d, '&' as e, '(' as f, '*' as g, '+' as h, ',' as i, '-' as j, '.' as k FORMAT Markdown;
SELECT '/' as a, ':' as b, ';' as c, '<' as d, '=' as e, '>' as f, '?' as g, '@' as h, '[' as i, '^' as j, '_' as k FORMAT Markdown;
SELECT '`' as a, '{' as b, '|' as c, '}' as d, '~' as e FORMAT Markdown;

SET output_format_markdown_escape_special_characters = false;
SELECT '!' as a, '#' as b, '$' as c, '%' as d, '&' as e, '(' as f, '*' as g, '+' as h, ',' as i, '-' as j, '.' as k FORMAT Markdown;
SELECT '/' as a, ':' as b, ';' as c, '<' as d, '=' as e, '>' as f, '?' as g, '@' as h, '[' as i, '^' as j, '_' as k FORMAT Markdown;
SELECT '`' as a, '{' as b, '|' as c, '}' as d, '~' as e FORMAT Markdown;
DROP TABLE IF EXISTS markdown;
