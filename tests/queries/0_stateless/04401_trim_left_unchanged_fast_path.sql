-- { echo }
-- trimLeft correctness: the common no-leading-space case (the perf hot path),
-- the mixed-column fallback, boundary inputs, FixedString, and trimRight/trimBoth/custom.

-- No row starts with a space: trimLeft is a no-op, result must equal input.
SELECT count() FROM numbers(100000) WHERE trimLeft(toString(number)) != toString(number);
SELECT count() FROM numbers(100000) WHERE ltrim(toString(number)) != toString(number);

-- Mixed column: some rows have leading spaces, which must still be removed.
SELECT trimLeft(s) FROM (SELECT arrayJoin(['abc', '  def', 'ghi', '   j', 'k']) AS s);

-- Boundary inputs: empty, single space, all spaces; leading removed, trailing/embedded kept.
SELECT trimLeft(''), trimLeft(' '), trimLeft('   '), trimLeft('a'), trimLeft('  a  '), trimLeft('a b c '), trimLeft('x  ');

-- Only the last row has a leading space.
SELECT trimLeft(s) FROM (SELECT arrayJoin(['a', 'b', 'c', ' z']) AS s) ORDER BY 1;

-- FixedString is handled by a separate path.
SELECT trimLeft(toFixedString('  hi', 4));

-- trimRight / trimBoth / custom-character trims.
SELECT trimRight('hello   '), trimBoth('  hi  '), trim(LEADING '0' FROM '000123'), trim(LEADING 'ab' FROM 'abcabc');
