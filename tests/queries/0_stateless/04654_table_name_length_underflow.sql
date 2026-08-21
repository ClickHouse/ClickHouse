-- The maximum table name length is bounded by the dropped-metadata filename
-- metadata_dropped/{db}.{table}.{uuid}.sql, so a long database name shrinks it.
-- Past the point where the database name alone fills the budget the limit must be 0,
-- not a wrapped unsigned value.

-- Boundary controls: these must not move.
SELECT 'esc_211', getMaxTableNameLengthForDatabase(repeat('d', 211));
SELECT 'esc_212', getMaxTableNameLengthForDatabase(repeat('d', 212));
SELECT 'esc_213', getMaxTableNameLengthForDatabase(repeat('d', 213));

-- The first length whose prefix exceeds the budget.
SELECT 'esc_214', getMaxTableNameLengthForDatabase(repeat('d', 214));

-- The limit is a function of the escaped length, not of the character count:
-- escapeForFileName expands every non-word byte to three characters, so 118
-- characters here escape to 70 + 48 * 3 = 214 bytes. A limit computed from the
-- unescaped length would report 95 instead of 0.
SELECT 'chars_118', length(repeat('d', 70) || repeat('-', 48));
SELECT 'esc_214_from_118_chars', getMaxTableNameLengthForDatabase(repeat('d', 70) || repeat('-', 48));

-- Far past the boundary.
SELECT 'esc_300', getMaxTableNameLengthForDatabase(repeat('d', 300));

-- Ordinary database name, unchanged.
SELECT 'esc_7', getMaxTableNameLengthForDatabase('default');
