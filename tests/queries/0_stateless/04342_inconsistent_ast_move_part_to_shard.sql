-- ALTER TABLE ... MOVE PART 'p' TO SHARD '/path' formatted back without the
-- SHARD keyword (the ASTAlterCommand formatter switch handled DISK/VOLUME/TABLE
-- but not SHARD), so format(query) produced "MOVE PART 'p' TO '/path'", which
-- fails to parse back and tripped the AST round-trip check with an
-- "Inconsistent AST formatting" LOGICAL_ERROR (abort in debug/sanitizer).

-- The formatted output must keep the SHARD keyword so it parses back.
SELECT formatQuery('ALTER TABLE x MOVE PART \'p\' TO SHARD \'/path\'');
SELECT formatQuerySingleLine('ALTER TABLE db.`my tbl` MOVE PART \'all_1_1_0\' TO SHARD \'/clickhouse/shard2\'');

-- Formatting the formatted output again must be stable (format == format(parse(format))).
SELECT formatQuery('ALTER TABLE x (MOVE PART \'p\' TO SHARD \'/path\')');

-- The other MOVE destinations must keep formatting correctly.
SELECT formatQuery('ALTER TABLE x MOVE PART \'p\' TO DISK \'d1\'');
SELECT formatQuery('ALTER TABLE x MOVE PARTITION 1 TO VOLUME \'v1\'');
SELECT formatQuery('ALTER TABLE x MOVE PARTITION 1 TO TABLE y');

-- SHARD is only valid for MOVE PART, not MOVE PARTITION.
SELECT formatQuery('ALTER TABLE x MOVE PARTITION 1 TO SHARD \'/path\''); -- { serverError SYNTAX_ERROR }
