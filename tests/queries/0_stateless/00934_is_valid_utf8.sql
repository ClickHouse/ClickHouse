select 1 = isValidUTF8('') from system.numbers limit 10;
select 1 = isValidUTF8('some text') from system.numbers limit 10;
select 1 = isValidUTF8('какой-то текст') from system.numbers limit 10;
select 1 = isValidUTF8('\x00') from system.numbers limit 10;
select 1 = isValidUTF8('\x66') from system.numbers limit 10;
select 1 = isValidUTF8('\x7F') from system.numbers limit 10;
select 1 = isValidUTF8('\x00\x7F') from system.numbers limit 10;
select 1 = isValidUTF8('\x7F\x00') from system.numbers limit 10;
select 1 = isValidUTF8('\xC2\x80') from system.numbers limit 10;
select 1 = isValidUTF8('\xDF\xBF') from system.numbers limit 10;
select 1 = isValidUTF8('\xE0\xA0\x80') from system.numbers limit 10;
select 1 = isValidUTF8('\xE0\xA0\xBF') from system.numbers limit 10;
select 1 = isValidUTF8('\xED\x9F\x80') from system.numbers limit 10;
select 1 = isValidUTF8('\xEF\x80\xBF') from system.numbers limit 10;
select 1 = isValidUTF8('\xF0\x90\xBF\x80') from system.numbers limit 10;
select 1 = isValidUTF8('\xF2\x81\xBE\x99') from system.numbers limit 10;
select 1 = isValidUTF8('\xF4\x8F\x88\xAA') from system.numbers limit 10;

select 1 = isValidUTF8('a') from system.numbers limit 10;
select 1 = isValidUTF8('\xc3\xb1') from system.numbers limit 10;
select 1 = isValidUTF8('\xe2\x82\xa1') from system.numbers limit 10;
select 1 = isValidUTF8('\xf0\x90\x8c\xbc') from system.numbers limit 10;
select 1 = isValidUTF8('안녕하세요, 세상') from system.numbers limit 10;

select 0 = isValidUTF8('\xc3\x28') from system.numbers limit 10;
select 0 = isValidUTF8('\xa0\xa1') from system.numbers limit 10;
select 0 = isValidUTF8('\xe2\x28\xa1') from system.numbers limit 10;
select 0 = isValidUTF8('\xe2\x82\x28') from system.numbers limit 10;
select 0 = isValidUTF8('\xf0\x28\x8c\xbc') from system.numbers limit 10;
select 0 = isValidUTF8('\xf0\x90\x28\xbc') from system.numbers limit 10;
select 0 = isValidUTF8('\xf0\x28\x8c\x28') from system.numbers limit 10;
select 0 = isValidUTF8('\xc0\x9f') from system.numbers limit 10;
select 0 = isValidUTF8('\xf5\xff\xff\xff') from system.numbers limit 10;
select 0 = isValidUTF8('\xed\xa0\x81') from system.numbers limit 10;
select 0 = isValidUTF8('\xf8\x90\x80\x80\x80') from system.numbers limit 10;
select 0 = isValidUTF8('12345678901234\xed') from system.numbers limit 10;
select 0 = isValidUTF8('123456789012345\xed') from system.numbers limit 10;
select 0 = isValidUTF8('123456789012345\xed123456789012345\xed') from system.numbers limit 10;
select 0 = isValidUTF8('123456789012345\xf1') from system.numbers limit 10;
select 0 = isValidUTF8('123456789012345\xc2') from system.numbers limit 10;
select 0 = isValidUTF8('\xC2\x7F') from system.numbers limit 10;

select 0 = isValidUTF8('\x80') from system.numbers limit 10;
select 0 = isValidUTF8('\xBF') from system.numbers limit 10;
select 0 = isValidUTF8('\xC0\x80') from system.numbers limit 10;
select 0 = isValidUTF8('\xC1\x00') from system.numbers limit 10;
select 0 = isValidUTF8('\xC2\x7F') from system.numbers limit 10;
select 0 = isValidUTF8('\xDF\xC0') from system.numbers limit 10;
select 0 = isValidUTF8('\xE0\x9F\x80') from system.numbers limit 10;
select 0 = isValidUTF8('\xE0\xC2\x80') from system.numbers limit 10;
select 0 = isValidUTF8('\xED\xA0\x80') from system.numbers limit 10;
select 0 = isValidUTF8('\xED\x7F\x80') from system.numbers limit 10;
select 0 = isValidUTF8('\xEF\x80\x00') from system.numbers limit 10;
select 0 = isValidUTF8('\xF0\x8F\x80\x80') from system.numbers limit 10;
select 0 = isValidUTF8('\xF0\xEE\x80\x80') from system.numbers limit 10;
select 0 = isValidUTF8('\xF2\x90\x91\x7F') from system.numbers limit 10;
select 0 = isValidUTF8('\xF4\x90\x88\xAA') from system.numbers limit 10;
select 0 = isValidUTF8('\xF4\x00\xBF\xBF') from system.numbers limit 10;
select 0 = isValidUTF8('\x00\x00\x00\x00\x00\xC2\x80\x00\x00\x00\xE1\x80\x80\x00\x00\xC2\xC2\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00') from system.numbers limit 10;
select 0 = isValidUTF8('\x00\x00\x00\x00\x00\xC2\xC2\x80\x00\x00\xE1\x80\x80\x00\x00\x00') from system.numbers limit 10;
select 0 = isValidUTF8('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80') from system.numbers limit 10;
select 0 = isValidUTF8('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1') from system.numbers limit 10;
select 0 = isValidUTF8('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80\x80') from system.numbers limit 10;
select 0 = isValidUTF8('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80\xC2\x80') from system.numbers limit 10;
select 0 = isValidUTF8('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF0\x80\x80\x80') from system.numbers limit 10;

select 1 = isValidUTF8(toFixedString('some text', 9)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('какой-то текст', 27)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\x00', 1)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\x66', 1)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\x7F', 1)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\x00\x7F', 2)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\x7F\x00', 2)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\xC2\x80', 2)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\xDF\xBF', 2)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\xE0\xA0\x80', 3)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\xE0\xA0\xBF', 3)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\xED\x9F\x80', 3)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\xEF\x80\xBF', 3)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\xF0\x90\xBF\x80', 4)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\xF2\x81\xBE\x99', 4)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\xF4\x8F\x88\xAA', 4)) from system.numbers limit 10;

select 0 = isValidUTF8(toFixedString('\x80', 1)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xBF', 1)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xC0\x80', 2)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xC1\x00', 2)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xC2\x7F', 2)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xDF\xC0', 2)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xE0\x9F\x80', 3)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xE0\xC2\x80', 3)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xED\xA0\x80', 3)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xED\x7F\x80', 3)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xEF\x80\x00', 3)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xF0\x8F\x80\x80', 4)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xF0\xEE\x80\x80', 4)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xF2\x90\x91\x7F', 4)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xF4\x90\x88\xAA', 4)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xF4\x00\xBF\xBF', 4)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\x00\x00\x00\x00\x00\xC2\x80\x00\x00\x00\xE1\x80\x80\x00\x00\xC2\xC2\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', 32)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\x00\x00\x00\x00\x00\xC2\xC2\x80\x00\x00\xE1\x80\x80\x00\x00\x00', 16)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80', 32)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1', 32)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80\x80', 33)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80\xC2\x80', 34)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF0\x80\x80\x80', 35)) from system.numbers limit 10;

select 1 = isValidUTF8(toFixedString('a', 1)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\xc3\xb1', 2)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\xe2\x82\xa1', 3)) from system.numbers limit 10;
select 1 = isValidUTF8(toFixedString('\xf0\x90\x8c\xbc', 4)) from system.numbers limit 10;

select 0 = isValidUTF8(toFixedString('\xc3\x28', 2)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xa0\xa1', 2)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xe2\x28\xa1', 3)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xe2\x82\x28', 3)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xf0\x28\x8c\xbc', 4)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xf0\x90\x28\xbc', 4)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xf0\x28\x8c\x28', 4)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xc0\x9f', 2)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xf5\xff\xff\xff', 4)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xed\xa0\x81', 3)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xf8\x90\x80\x80\x80', 5)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('123456789012345\xed', 16)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('123456789012345\xf1', 16)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('123456789012345\xc2', 16)) from system.numbers limit 10;
select 0 = isValidUTF8(toFixedString('\xC2\x7F', 2)) from system.numbers limit 10;

-- Lengths around the boundaries where the vectorised implementations switch behaviour.
select n, 1 = isValidUTF8(repeat('a', n)) from (select arrayJoin([0, 1, 2, 3, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129]) as n) order by n;

-- A multibyte sequence straddling those boundaries, well-formed and truncated, inside a string long enough to take the vector path.
select off, 1 = isValidUTF8(repeat('x', off) || '\xE2\x82\xA1' || repeat('y', 100)), 0 = isValidUTF8(repeat('x', off) || '\xE2\x82' || repeat('y', 100)) from (select arrayJoin([0, 13, 14, 15, 16, 17, 47, 61, 62, 63, 64, 65]) as off) order by off;

-- A truncated lead byte at the end of a block followed by a full block of ASCII: an ASCII-only fast path that ignores carried state accepts this.
select 0 = isValidUTF8(repeat('x', 15) || '\xC2' || repeat('y', 16));
select 0 = isValidUTF8(repeat('x', 15) || '\xC2' || repeat('y', 48));
select 0 = isValidUTF8(repeat('x', 63) || '\xC2' || repeat('y', 64));
select 0 = isValidUTF8(repeat('x', 100) || '\xE0\xA0' || repeat('y', 100));
select 0 = isValidUTF8(repeat('x', 100) || '\xF0\x90\x80' || repeat('y', 100));

-- Malformed sequences at the end of a string long enough to take the vector path.
select 0 = isValidUTF8(repeat('a', 100) || '\xC2');
select 0 = isValidUTF8(repeat('a', 100) || '\xE0\xA0');
select 0 = isValidUTF8(repeat('a', 100) || '\xF4\x8F\x88');

-- Malformed classes that a range check must reject, embedded in a long string.
select 0 = isValidUTF8(repeat('a', 64) || '\xC0\x80' || repeat('a', 64));
select 0 = isValidUTF8(repeat('a', 64) || '\xC1\xBF' || repeat('a', 64));
select 0 = isValidUTF8(repeat('a', 64) || '\xE0\x80\x80' || repeat('a', 64));
select 0 = isValidUTF8(repeat('a', 64) || '\xF0\x80\x80\x80' || repeat('a', 64));
select 0 = isValidUTF8(repeat('a', 64) || '\xED\xA0\x80' || repeat('a', 64));
select 0 = isValidUTF8(repeat('a', 64) || '\xED\xBF\xBF' || repeat('a', 64));
select 0 = isValidUTF8(repeat('a', 64) || '\xF4\x90\x80\x80' || repeat('a', 64));
select 0 = isValidUTF8(repeat('a', 64) || '\xF5\x80\x80\x80' || repeat('a', 64));
select 0 = isValidUTF8(repeat('a', 64) || '\x80' || repeat('a', 64));
select 0 = isValidUTF8(repeat('a', 64) || '\xBF' || repeat('a', 64));
select 0 = isValidUTF8(repeat('a', 64) || '\xFE' || repeat('a', 64));
select 0 = isValidUTF8(repeat('a', 64) || '\xFF' || repeat('a', 64));

-- Well-formed sequences of every length must still be accepted in a long string.
select 1 = isValidUTF8(repeat('a', 64) || '\xC2\x80' || repeat('a', 64));
select 1 = isValidUTF8(repeat('a', 64) || '\xDF\xBF' || repeat('a', 64));
select 1 = isValidUTF8(repeat('a', 64) || '\xE0\xA0\x80' || repeat('a', 64));
select 1 = isValidUTF8(repeat('a', 64) || '\xED\x9F\xBF' || repeat('a', 64));
select 1 = isValidUTF8(repeat('a', 64) || '\xEE\x80\x80' || repeat('a', 64));
select 1 = isValidUTF8(repeat('a', 64) || '\xF0\x90\x80\x80' || repeat('a', 64));
select 1 = isValidUTF8(repeat('a', 64) || '\xF4\x8F\xBF\xBF' || repeat('a', 64));
select 1 = isValidUTF8(repeat('\xD0\xB0', 100));
select 1 = isValidUTF8(repeat('\xE2\x82\xAC', 100));
select 1 = isValidUTF8(repeat('\xF0\x9F\x98\x80', 100));

-- The same boundaries through FixedString, which reaches the implementation via a different caller.
select 1 = isValidUTF8(toFixedString(repeat('a', 1), 1));
select 1 = isValidUTF8(toFixedString(repeat('a', 15), 15));
select 1 = isValidUTF8(toFixedString(repeat('a', 16), 16));
select 1 = isValidUTF8(toFixedString(repeat('a', 17), 17));
select 1 = isValidUTF8(toFixedString(repeat('a', 31), 31));
select 1 = isValidUTF8(toFixedString(repeat('a', 32), 32));
select 1 = isValidUTF8(toFixedString(repeat('a', 33), 33));
select 1 = isValidUTF8(toFixedString(repeat('a', 63), 63));
select 1 = isValidUTF8(toFixedString(repeat('a', 64), 64));
select 1 = isValidUTF8(toFixedString(repeat('a', 65), 65));
select 1 = isValidUTF8(toFixedString(repeat('a', 127), 127));
select 1 = isValidUTF8(toFixedString(repeat('a', 128), 128));
select 1 = isValidUTF8(toFixedString(repeat('a', 129), 129));
select 1 = isValidUTF8(toFixedString(repeat('a', 63) || '\xE2\x82\xAC' || repeat('a', 64), 130));
select 0 = isValidUTF8(toFixedString(repeat('a', 63) || '\xE2\x82' || repeat('a', 65), 130));
select 0 = isValidUTF8(toFixedString(repeat('x', 15) || '\xC2' || repeat('y', 48), 64));
select 0 = isValidUTF8(toFixedString(repeat('a', 64) || '\xED\xA0\x80' || repeat('a', 64), 131));
select 0 = isValidUTF8(toFixedString(repeat('a', 100) || '\xC2', 101));
