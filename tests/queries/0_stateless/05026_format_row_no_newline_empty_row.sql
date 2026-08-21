-- `formatRowNoNewline` writes all the rows into one buffer, so a row that produced no output at
-- all must not strip a newline belonging to the previous row: that would make the offsets of the
-- result column non-monotonic and the size of the previous row underflow.

SELECT hex(formatRowNoNewline('RawBLOB', s)), length(formatRowNoNewline('RawBLOB', s))
FROM (SELECT arrayJoin([char(97, 10, 10), '']) AS s);

SELECT hex(substring(formatRowNoNewline('RawBLOB', s), 1, 16))
FROM (SELECT arrayJoin([char(97, 10, 10), '']) AS s);

SELECT formatRowNoNewline('CSV', s) FROM (SELECT arrayJoin(['a', '', 'b']) AS s);
