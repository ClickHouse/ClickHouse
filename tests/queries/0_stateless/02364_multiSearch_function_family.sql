SET send_logs_level = 'fatal';

select 0 = multiSearchAny('\0', CAST([], 'Array(String)'));
select 0 = multiSearchAnyCaseInsensitive('\0', CAST([], 'Array(String)'));
select 0 = multiSearchAnyCaseInsensitiveUTF8('\0', CAST([], 'Array(String)'));
select 0 = multiSearchAnyUTF8('\0', CAST([], 'Array(String)'));
select 0 = multiSearchFirstIndex('\0', CAST([], 'Array(String)'));
select 0 = multiSearchFirstIndexCaseInsensitive('\0', CAST([], 'Array(String)'));
select 0 = multiSearchFirstIndexCaseInsensitiveUTF8('\0', CAST([], 'Array(String)'));
select 0 = multiSearchFirstIndexUTF8('\0', CAST([], 'Array(String)'));
select 0 = multiSearchFirstPosition('\0', CAST([], 'Array(String)'));
select 0 = multiSearchFirstPositionCaseInsensitive('\0', CAST([], 'Array(String)'));
select 0 = multiSearchFirstPositionCaseInsensitiveUTF8('\0', CAST([], 'Array(String)'));
select 0 = multiSearchFirstPositionUTF8('\0', CAST([], 'Array(String)'));
select [] = multiSearchAllPositions('\0', CAST([], 'Array(String)'));
select [] = multiSearchAllPositionsCaseInsensitive('\0', CAST([], 'Array(String)'));
select [] = multiSearchAllPositionsCaseInsensitiveUTF8('\0', CAST([], 'Array(String)'));
select [] = multiSearchAllPositionsUTF8('\0', CAST([], 'Array(String)'));

select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['b']);
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bc']);
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcd']);
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcde']);
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdef']);
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdefg']);
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdefgh']);

select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdefgh']);
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdefg']);
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdef']);
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcde']);
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcd']);
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abc']);
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['ab']);
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['a']);

select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['c']);
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cd']);
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cde']);
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdef']);
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdefg']);
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdefgh']);

select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['defgh']);
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['defg']);
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['def']);
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['de']);
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['d']);

select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['e']);
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['ef']);
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['efg']);
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['efgh']);

select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['fgh']);
select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['fg']);
select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['f']);

select [7] = multiSearchAllPositions(materialize('abcdefgh'), ['g']);
select [7] = multiSearchAllPositions(materialize('abcdefgh'), ['gh']);

select [8] = multiSearchAllPositions(materialize('abcdefgh'), ['h']);

select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['b']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bc']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcd']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcde']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdef']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdefg']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdefgh']) from system.numbers limit 10;

select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdefgh']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdefg']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdef']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcde']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcd']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abc']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['ab']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['a']) from system.numbers limit 10;

select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['c']) from system.numbers limit 10;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cd']) from system.numbers limit 10;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cde']) from system.numbers limit 10;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdef']) from system.numbers limit 10;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdefg']) from system.numbers limit 10;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdefgh']) from system.numbers limit 10;

select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['defgh']) from system.numbers limit 10;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['defg']) from system.numbers limit 10;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['def']) from system.numbers limit 10;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['de']) from system.numbers limit 10;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['d']) from system.numbers limit 10;

select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['e']) from system.numbers limit 10;
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['ef']) from system.numbers limit 10;
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['efg']) from system.numbers limit 10;
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['efgh']) from system.numbers limit 10;

select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['fgh']) from system.numbers limit 10;
select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['fg']) from system.numbers limit 10;
select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['f']) from system.numbers limit 10;

select [7] = multiSearchAllPositions(materialize('abcdefgh'), ['g']) from system.numbers limit 10;
select [7] = multiSearchAllPositions(materialize('abcdefgh'), ['gh']) from system.numbers limit 10;

select [8] = multiSearchAllPositions(materialize('abcdefgh'), ['h']) from system.numbers limit 10;

select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['b']) from system.numbers limit 129;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bc']) from system.numbers limit 129;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcd']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcde']) from system.numbers limit 129;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdef']) from system.numbers limit 129;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdefg']) from system.numbers limit 129;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdefgh']) from system.numbers limit 129;

select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdefgh']) from system.numbers limit 129;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdefg']) from system.numbers limit 129;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdef']) from system.numbers limit 129;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcde']) from system.numbers limit 129;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcd']) from system.numbers limit 129;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abc']) from system.numbers limit 129;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['ab']) from system.numbers limit 129;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['a']) from system.numbers limit 129;

select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['c']) from system.numbers limit 129;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cd']) from system.numbers limit 129;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cde']) from system.numbers limit 129;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdef']) from system.numbers limit 129;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdefg']) from system.numbers limit 129;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdefgh']) from system.numbers limit 129;

select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['defgh']) from system.numbers limit 129;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['defg']) from system.numbers limit 129;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['def']) from system.numbers limit 129;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['de']) from system.numbers limit 129;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['d']) from system.numbers limit 129;

select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['e']) from system.numbers limit 129;
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['ef']) from system.numbers limit 129;
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['efg']) from system.numbers limit 129;
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['efgh']) from system.numbers limit 129;

select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['fgh']) from system.numbers limit 129;
select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['fg']) from system.numbers limit 129;
select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['f']) from system.numbers limit 129;

select [7] = multiSearchAllPositions(materialize('abcdefgh'), ['g']) from system.numbers limit 129;
select [7] = multiSearchAllPositions(materialize('abcdefgh'), ['gh']) from system.numbers limit 129;

select [8] = multiSearchAllPositions(materialize('abcdefgh'), ['h']) from system.numbers limit 129;

select [2] = multiSearchAllPositions(materialize('abc'), ['b']);
select [2] = multiSearchAllPositions(materialize('abc'), ['bc']);
select [0] = multiSearchAllPositions(materialize('abc'), ['bcde']);
select [0] = multiSearchAllPositions(materialize('abc'), ['bcdef']);
select [0] = multiSearchAllPositions(materialize('abc'), ['bcdefg']);
select [0] = multiSearchAllPositions(materialize('abc'), ['bcdefgh']);

select [0] = multiSearchAllPositions(materialize('abc'), ['abcdefg']);
select [0] = multiSearchAllPositions(materialize('abc'), ['abcdef']);
select [0] = multiSearchAllPositions(materialize('abc'), ['abcde']);
select [0] = multiSearchAllPositions(materialize('abc'), ['abcd']);
select [1] = multiSearchAllPositions(materialize('abc'), ['abc']);
select [1] = multiSearchAllPositions(materialize('abc'), ['ab']);
select [1] = multiSearchAllPositions(materialize('abc'), ['a']);

select [3] = multiSearchAllPositions(materialize('abcd'), ['c']);
select [3] = multiSearchAllPositions(materialize('abcd'), ['cd']);
select [0] = multiSearchAllPositions(materialize('abcd'), ['cde']);
select [0] = multiSearchAllPositions(materialize('abcd'), ['cdef']);
select [0] = multiSearchAllPositions(materialize('abcd'), ['cdefg']);
select [0] = multiSearchAllPositions(materialize('abcd'), ['cdefgh']);

select [0] = multiSearchAllPositions(materialize('abc'), ['defgh']);
select [0] = multiSearchAllPositions(materialize('abc'), ['defg']);
select [0] = multiSearchAllPositions(materialize('abc'), ['def']);
select [0] = multiSearchAllPositions(materialize('abc'), ['de']);
select [0] = multiSearchAllPositions(materialize('abc'), ['d']);


select [2] = multiSearchAllPositions(materialize('abc'), ['b']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abc'), ['bc']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['bcde']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['bcdef']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['bcdefg']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['bcdefgh']) from system.numbers limit 10;


select [0] = multiSearchAllPositions(materialize('abc'), ['abcdefg']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['abcdef']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['abcde']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['abcd']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abc'), ['abc']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abc'), ['ab']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abc'), ['a']) from system.numbers limit 10;

select [3] = multiSearchAllPositions(materialize('abcd'), ['c']) from system.numbers limit 10;
select [3] = multiSearchAllPositions(materialize('abcd'), ['cd']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abcd'), ['cde']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abcd'), ['cdef']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abcd'), ['cdefg']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abcd'), ['cdefgh']) from system.numbers limit 10;

select [0] = multiSearchAllPositions(materialize('abc'), ['defgh']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['defg']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['def']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['de']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['d']) from system.numbers limit 10;

select [1] = multiSearchAllPositions(materialize('abc'), ['']);
select [1] = multiSearchAllPositions(materialize('abc'), ['']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abc'), ['']) from system.numbers limit 100;
select [1] = multiSearchAllPositions(materialize('abc'), ['']) from system.numbers limit 1000;

select [1] = multiSearchAllPositions(materialize('abab'), ['ab']);
select [1] = multiSearchAllPositions(materialize('abababababababababababab'), ['abab']);
select [1] = multiSearchAllPositions(materialize('abababababababababababab'), ['abababababababababa']);

select [1] = multiSearchAllPositions(materialize('abc'), materialize(['']));
select [1] = multiSearchAllPositions(materialize('abc'), materialize([''])) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abab'), materialize(['ab']));
select [2] = multiSearchAllPositions(materialize('abab'), materialize(['ba']));
select [1] = multiSearchAllPositionsCaseInsensitive(materialize('aBaB'), materialize(['abab']));
select [3] = multiSearchAllPositionsUTF8(materialize('ab€ab'), materialize(['€']));
select [3] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('ab€AB'), materialize(['€ab']));
-- checks the correct handling of broken utf-8 sequence
select [0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize(''), materialize(['a\x90\x90\x90\x90\x90\x90']));

select 1 = multiSearchAny(materialize('abcdefgh'), ['b']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['bc']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcd']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcde']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdef']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdefg']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdefgh']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdefgh']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdefg']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdef']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcde']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcd']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['abc']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['ab']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['a']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['c']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['cd']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['cde']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdef']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdefg']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdefgh']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['defgh']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['defg']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['def']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['de']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['d']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['e']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['ef']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['efg']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['efgh']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['fgh']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['fg']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['f']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['g']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['gh']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['h']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['b']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bc']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcd']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcde']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdef']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdefg']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdefgh']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdefgh']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdefg']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdef']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcde']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcd']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abc']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['ab']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['a']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['c']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cd']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cde']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdef']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdefg']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdefgh']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['defgh']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['defg']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['def']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['de']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['d']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['e']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['ef']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['efg']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['efgh']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['fgh']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['fg']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['f']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['g']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['gh']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['h']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['b']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bc']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcd']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcde']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdef']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdefg']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdefgh']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdefgh']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdefg']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdef']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcde']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcd']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abc']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['ab']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['a']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abcdefgh'), ['c']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cd']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cde']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdef']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdefg']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdefgh']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abcdefgh'), ['defgh']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['defg']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['def']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['de']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['d']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abcdefgh'), ['e']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['ef']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['efg']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['efgh']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abcdefgh'), ['fgh']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['fg']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['f']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abcdefgh'), ['g']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['gh']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abcdefgh'), ['h']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abc'), ['b']);
select 1 = multiSearchAny(materialize('abc'), ['bc']);
select 0 = multiSearchAny(materialize('abc'), ['bcde']);
select 0 = multiSearchAny(materialize('abc'), ['bcdef']);
select 0 = multiSearchAny(materialize('abc'), ['bcdefg']);
select 0 = multiSearchAny(materialize('abc'), ['bcdefgh']);

select 0 = multiSearchAny(materialize('abc'), ['abcdefg']);
select 0 = multiSearchAny(materialize('abc'), ['abcdef']);
select 0 = multiSearchAny(materialize('abc'), ['abcde']);
select 0 = multiSearchAny(materialize('abc'), ['abcd']);
select 1 = multiSearchAny(materialize('abc'), ['abc']);
select 1 = multiSearchAny(materialize('abc'), ['ab']);
select 1 = multiSearchAny(materialize('abc'), ['a']);

select 1 = multiSearchAny(materialize('abcd'), ['c']);
select 1 = multiSearchAny(materialize('abcd'), ['cd']);
select 0 = multiSearchAny(materialize('abcd'), ['cde']);
select 0 = multiSearchAny(materialize('abcd'), ['cdef']);
select 0 = multiSearchAny(materialize('abcd'), ['cdefg']);
select 0 = multiSearchAny(materialize('abcd'), ['cdefgh']);

select 0 = multiSearchAny(materialize('abc'), ['defgh']);
select 0 = multiSearchAny(materialize('abc'), ['defg']);
select 0 = multiSearchAny(materialize('abc'), ['def']);
select 0 = multiSearchAny(materialize('abc'), ['de']);
select 0 = multiSearchAny(materialize('abc'), ['d']);


select 1 = multiSearchAny(materialize('abc'), ['b']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abc'), ['bc']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['bcde']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['bcdef']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['bcdefg']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['bcdefgh']) from system.numbers limit 10;


select 0 = multiSearchAny(materialize('abc'), ['abcdefg']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['abcdef']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['abcde']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['abcd']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abc'), ['abc']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abc'), ['ab']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abc'), ['a']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcd'), ['c']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcd'), ['cd']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abcd'), ['cde']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abcd'), ['cdef']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abcd'), ['cdefg']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abcd'), ['cdefgh']) from system.numbers limit 10;

select 0 = multiSearchAny(materialize('abc'), ['defgh']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['defg']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['def']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['de']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['d']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abc'), ['']);
select 1 = multiSearchAny(materialize('abc'), ['']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abc'), ['']) from system.numbers limit 100;
select 1 = multiSearchAny(materialize('abc'), ['']) from system.numbers limit 1000;

select 1 = multiSearchAny(materialize('abab'), ['ab']);
select 1 = multiSearchAny(materialize('abababababababababababab'), ['abab']);
select 1 = multiSearchAny(materialize('abababababababababababab'), ['abababababababababa']);


select 0 = multiSearchFirstPosition(materialize('abcdefgh'), ['z', 'pq']) from system.numbers limit 10;
select 1 = multiSearchFirstPosition(materialize('abcdefgh'), ['a', 'b', 'c', 'd']) from system.numbers limit 10;
select 1 = multiSearchFirstPosition(materialize('abcdefgh'), ['defgh', 'bcd', 'abcd', 'c']) from system.numbers limit 10;
select 1 = multiSearchFirstPosition(materialize('abcdefgh'), ['', 'bcd', 'bcd', 'c']) from system.numbers limit 10;
select 2 = multiSearchFirstPosition(materialize('abcdefgh'), ['something', 'bcd', 'bcd', 'c']) from system.numbers limit 10;
select 6 = multiSearchFirstPosition(materialize('abcdefgh'), ['something', 'bcdz', 'fgh', 'f']) from system.numbers limit 10;

select 0 = multiSearchFirstPositionCaseInsensitive(materialize('abcdefgh'), ['z', 'pq']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionCaseInsensitive(materialize('aBcdefgh'), ['A', 'b', 'c', 'd']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionCaseInsensitive(materialize('abCDefgh'), ['defgh', 'bcd', 'aBCd', 'c']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionCaseInsensitive(materialize('abCdeFgH'), ['', 'bcd', 'bcd', 'c']) from system.numbers limit 10;
select 2 = multiSearchFirstPositionCaseInsensitive(materialize('ABCDEFGH'), ['something', 'bcd', 'bcd', 'c']) from system.numbers limit 10;
select 6 = multiSearchFirstPositionCaseInsensitive(materialize('abcdefgh'), ['sOmEthIng', 'bcdZ', 'fGh', 'F']) from system.numbers limit 10;

select 0 = multiSearchFirstPositionUTF8(materialize('абвгдежз'), ['л', 'ъ']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionUTF8(materialize('абвгдежз'), ['а', 'б', 'в', 'г']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionUTF8(materialize('абвгдежз'), ['гдежз', 'бвг', 'абвг', 'вг']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionUTF8(materialize('абвгдежз'), ['', 'бвг', 'бвг', 'в']) from system.numbers limit 10;
select 2 = multiSearchFirstPositionUTF8(materialize('абвгдежз'), ['что', 'в', 'гдз', 'бвг']) from system.numbers limit 10;
select 6 = multiSearchFirstPositionUTF8(materialize('абвгдежз'), ['з', 'бвгя', 'ежз', 'з']) from system.numbers limit 10;

select 0 = multiSearchFirstPositionCaseInsensitiveUTF8(materialize('аБвгДежз'), ['Л', 'Ъ']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionCaseInsensitiveUTF8(materialize('аБвгДежз'), ['А', 'б', 'в', 'г']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionCaseInsensitiveUTF8(materialize('аБвгДежз'), ['гДеЖз', 'бВг', 'АБВг', 'вг']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionCaseInsensitiveUTF8(materialize('аБвгДежз'), ['', 'бвг', 'Бвг', 'в']) from system.numbers limit 10;
select 2 = multiSearchFirstPositionCaseInsensitiveUTF8(materialize('аБвгДежз'), ['что', 'в', 'гдз', 'бвг']) from system.numbers limit 10;
select 6 = multiSearchFirstPositionCaseInsensitiveUTF8(materialize('аБвгДежЗ'), ['З', 'бвгЯ', 'ЕЖз', 'з']) from system.numbers limit 10;

select
[
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
] =
multiSearchAllPositions(materialize('string'),
['o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'str']);

select 254 = multiSearchFirstIndex(materialize('string'),
['o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'str']);


select
[
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
] =
multiSearchAllPositions(materialize('string'),
['o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'str']);

select 255 = multiSearchFirstIndex(materialize('string'),
['o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'str']);

select multiSearchAllPositions(materialize('string'),
['o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'str']); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

select multiSearchFirstIndex(materialize('string'),
['o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'str']); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
