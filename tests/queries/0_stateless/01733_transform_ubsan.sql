SELECT arrayStringConcat(arrayMap(x -> transform(x, [1025, -9223372036854775808, 65537, 257, 1048576, 10, 7, 1048575, 65536], ['censor.net', 'googlegooglegooglegoogle', 'test', '', '', 'hello', 'world', '', 'xyz'], ''), arrayMap(x -> (x % -inf), range(number))), '')
FROM system.numbers
LIMIT 1025
FORMAT Null;
