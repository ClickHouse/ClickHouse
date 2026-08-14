-- Tags: no-fasttest
-- no-fasttest: the Arrow format is not available in fasttest builds

-- The Arrow spec leaves the value bytes of a null slot undefined, so a value hidden under a null
-- slot must not fail value-level validation on read; it decodes as the type default. ClickHouse
-- itself produces such files: Date32 arithmetic can push the raw day number past the type's
-- supported range without clamping the storage, and nullIf keeps the original payload under the
-- new null mask, so the Arrow writer stores an out-of-range day number under a null slot.

SET engine_file_truncate_on_insert = 1;
SET allow_experimental_nullable_tuple_type = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_04512_plain.arrow', 'Arrow')
SELECT nullIf(x, x) AS d FROM (SELECT toDate32('9999-12-31') + 100 AS x);
INSERT INTO FUNCTION file(currentDatabase() || '_04512_plain.arrowstream', 'ArrowStream')
SELECT nullIf(x, x) AS d FROM (SELECT toDate32('9999-12-31') + 100 AS x);

-- The same payload inside a nullable struct: only the struct-level validity marks the row null,
-- the child's own validity does not, so the reader must compose ancestor validity to see it.
INSERT INTO FUNCTION file(currentDatabase() || '_04512_struct.arrow', 'Arrow')
SELECT nullIf(t, t) AS s FROM (SELECT CAST(tuple(toDate32('9999-12-31') + 100), 'Tuple(d Date32)') AS t);

-- A visible out-of-range value: the last two queries check that validation still rejects real data
-- and that date_time_overflow_behavior = 'saturate' still clamps it. The saturated value is printed
-- as the raw day number: the boundary day sits at the edge of the DateLUT, whose rendering differs
-- across builds.
INSERT INTO FUNCTION file(currentDatabase() || '_04512_visible.arrow', 'Arrow')
SELECT toDate32('9999-12-31') + 100 AS d;

-- { echoOn }

SELECT * FROM file(currentDatabase() || '_04512_plain.arrow', 'Arrow');
SELECT * FROM file(currentDatabase() || '_04512_plain.arrowstream', 'ArrowStream');

SELECT * FROM file(currentDatabase() || '_04512_struct.arrow', 'Arrow');

SELECT * FROM file(currentDatabase() || '_04512_visible.arrow', 'Arrow'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toInt32(d) FROM file(currentDatabase() || '_04512_visible.arrow', 'Arrow', 'd Date32') SETTINGS date_time_overflow_behavior = 'saturate';
