-- Cases ported from the former unit tests of the key-value pair extractor, plus inputs with long tokens
-- (to exercise the vectorized scanning of the extractor) and non-ASCII bytes.

SET output_format_pretty_single_large_number_tip_threshold = 0;

-- { echoOn }

-- standard escape sequences are decoded, the result is checked byte by byte
SELECT hex(x['key1']), hex(x['key2']) FROM (SELECT extractKeyValuePairsWithEscaping('key1:a\\xFF key2:a\\n\\t\\r') AS x);
SELECT extractKeyValuePairsWithEscaping('age:a\\x0A\\n\\0');
SELECT extractKeyValuePairs('age:a\\x0A\\n\\0');

-- quoting character inside a quoted value must be escaped
SELECT extractKeyValuePairsWithEscaping('a:"aaaa\\"bbb" c:d');
SELECT extractKeyValuePairs('a:"aaaa\\"bbb" c:d');

-- non-standard escape sequences keep the backslash
SELECT extractKeyValuePairsWithEscaping('ke\\y:va\\lue k\\x41:v');

-- invalid escape sequence at the end of the value: best effort, the value is kept
SELECT extractKeyValuePairsWithEscaping('valid_key:valid_value key:invalid_val\\');
SELECT extractKeyValuePairs('valid_key:valid_value key:invalid_val\\');
SELECT extractKeyValuePairsWithEscaping('valid_key:valid_value key:invalid_val\\ third_key:third_value');
SELECT extractKeyValuePairs('valid_key:valid_value key:invalid_val\\ third_key:third_value');
SELECT extractKeyValuePairsWithEscaping('valid_key:valid_value key:"invalid val\\ " "third key":"third value"');
SELECT extractKeyValuePairs('valid_key:valid_value key:"invalid val\\ " "third key":"third value"');

-- incomplete hex escape sequence
SELECT extractKeyValuePairsWithEscaping('k\\x4');
SELECT extractKeyValuePairsWithEscaping('k\\x4:v a:b');
SELECT extractKeyValuePairsWithEscaping('a:v\\x4');
SELECT extractKeyValuePairsWithEscaping('a:"v\\x4" b:c');

-- leading escape sequences are skipped in keys and are invalid for values
SELECT extractKeyValuePairsWithEscaping('\\:name:val');
SELECT extractKeyValuePairsWithEscaping('\\\\"name":val');
SELECT extractKeyValuePairsWithEscaping('a:\\N b:c');
SELECT extractKeyValuePairsWithEscaping('a:x\\N b:c');
SELECT extractKeyValuePairsWithEscaping('"\\N":5 a:b');

-- escape sequences inside quoted keys
SELECT extractKeyValuePairsWithEscaping('"name\\n\\x4E":1');
SELECT hex(x['name\nN']) FROM (SELECT extractKeyValuePairsWithEscaping('"name\\n\\x4E":1') AS x);

-- single quote as quoting character, both variants
SELECT extractKeyValuePairs('name:\'neymar\';\'age\':31;team:psg;nationality:brazil,last_key:last_value', ':', ';,', '\'');
SELECT extractKeyValuePairsWithEscaping('name:\'neymar\';\'age\':31;team:psg;nationality:brazil,last_key:last_value', ':', ';,', '\'');

-- unexpected quoting character strategies with escaping
SELECT extractKeyValuePairsWithEscaping('name"abc:5', ':', ' ,;', '"', 'INVALID');
SELECT extractKeyValuePairsWithEscaping('name"abc":5', ':', ' ,;', '"', 'INVALID');
SELECT extractKeyValuePairsWithEscaping('name"abc:5', ':', ' ,;', '"', 'ACCEPT');
SELECT extractKeyValuePairsWithEscaping('name"abc":5', ':', ' ,;', '"', 'ACCEPT');
SELECT extractKeyValuePairsWithEscaping('name"abc:5', ':', ' ,;', '"', 'PROMOTE');
SELECT extractKeyValuePairsWithEscaping('name"abc":5', ':', ' ,;', '"', 'PROMOTE');
SELECT extractKeyValuePairsWithEscaping('k:v"x y:z', ':', ' ,;', '"', 'INVALID');
SELECT extractKeyValuePairsWithEscaping('k:v"x y:z', ':', ' ,;', '"', 'ACCEPT');
SELECT extractKeyValuePairsWithEscaping('k:v"x y:z', ':', ' ,;', '"', 'PROMOTE');

-- long tokens: keys and values longer than the vector width, delimiters at various offsets
SELECT extractKeyValuePairs(concat(repeat('k', 100), ':', repeat('v', 300), ' ', repeat('a', 17), ':', repeat('b', 15), ',', repeat('c', 16), ':', repeat('d', 16)));
SELECT extractKeyValuePairsWithEscaping(concat(repeat('k', 100), ':', repeat('v', 300), ' ', repeat('a', 17), ':', repeat('b', 15), ',', repeat('c', 16), ':', repeat('d', 16)));
SELECT extractKeyValuePairs(concat('"', repeat('q', 50), '":"', repeat('w', 70), '"', repeat(' ', 40), 'z:1'));
SELECT extractKeyValuePairsWithEscaping(concat('"', repeat('q', 50), '":"', repeat('w', 30), '\\t', repeat('w', 30), '"', repeat(' ', 40), 'z:1'));
SELECT extractKeyValuePairsWithEscaping(concat('k:', repeat('v', 200), '\\n', repeat('v', 200), ' a:b'));
SELECT extractKeyValuePairs(concat(repeat(':', 17), 'a:b', repeat(' ', 31)));
SELECT extractKeyValuePairs(repeat('x', 100));
SELECT extractKeyValuePairs(concat(repeat('x', 100), ':', repeat('y', 100)));
SELECT length(extractKeyValuePairs(concat('k:', repeat('a', 2000), ' q:"', repeat('b', 2000), '" z:1')));
SELECT arraySort(mapKeys(extractKeyValuePairs(arrayStringConcat(arrayMap(i -> concat('k', toString(i), ':', toString(i)), range(30)), ' '))));

-- non-ASCII bytes in keys and values
SELECT extractKeyValuePairs('ключ:значение, 键:值 "цитата":"кавычки"');
SELECT extractKeyValuePairsWithEscaping('ключ:значение, 键:值 "цитата":"кавычки"');
SELECT extractKeyValuePairs('a:\xff\xfe b:\x80');

-- delimiters with bytes outside ASCII
SELECT extractKeyValuePairs('a\x80b\x91c\xa2d', '\x80', '\x91\xa2', '\xb3');
SELECT extractKeyValuePairs('k1\xf0v1\x80k2\xf0\xb3v 2\xb3\x91k3\xf0v3', '\xf0', '\x80\x91\xa2\xc4\xd5\xe6\x0a\x09', '\xb3');
SELECT extractKeyValuePairsWithEscaping('k1\xf0v1\x80k2\xf0\xb3v 2\xb3\x91k3\xf0v\\n3', '\xf0', '\x80\x91\xa2\xc4\xd5\xe6\x0a\x09', '\xb3');

-- FixedString input
SELECT extractKeyValuePairs(toFixedString('a:b c:d', 10));

-- multiple rows, mix of empty and non-empty strings
SELECT extractKeyValuePairs(s), extractKeyValuePairsWithEscaping(s) FROM (SELECT arrayJoin(['', 'a:b', ' ', 'k:"v', 'x:y\\', '::', 'a:b:c d']) AS s);
