-- `date_part('unit', expr)` is syntactic sugar for `EXTRACT(unit FROM expr)`
-- and produces the same AST as EXTRACT, so the standard interval kinds and the
-- PostgreSQL-specific extra units (epoch, dow, doy, isodow, isoyear, century,
-- decade, millennium) are all supported.

SELECT date_part('year', toDate('2024-03-17'));
SELECT date_part('month', toDate('2024-03-17'));
SELECT date_part('day', toDate('2024-03-17'));
SELECT date_part('hour', toDateTime('2024-03-17 13:45:07'));
SELECT date_part('minute', toDateTime('2024-03-17 13:45:07'));
SELECT date_part('second', toDateTime('2024-03-17 13:45:07'));
SELECT date_part('week', toDate('2024-03-17'));
SELECT date_part('quarter', toDate('2024-03-17'));

-- Case-insensitive unit string.
SELECT date_part('YEAR', toDate('2024-03-17'));
SELECT date_part('Month', toDate('2024-03-17'));

-- PostgreSQL extra units.
SELECT date_part('epoch', toDateTime('2024-03-17 00:00:00', 'UTC'));
SELECT date_part('dow', toDate('2024-03-17'));    -- Sunday = 0
SELECT date_part('isodow', toDate('2024-03-17')); -- Sunday = 7
SELECT date_part('doy', toDate('2024-03-17'));
SELECT date_part('isoyear', toDate('2024-03-17'));
SELECT date_part('century', toDate('2024-03-17'));
SELECT date_part('decade', toDate('2024-03-17'));
SELECT date_part('millennium', toDate('2024-03-17'));

-- `datepart` (no underscore) is also accepted.
SELECT datepart('year', toDate('2024-03-17'));

-- Equivalence with EXTRACT(unit FROM expr).
SELECT date_part('year', toDate('2024-03-17')) = EXTRACT(YEAR FROM toDate('2024-03-17'));
SELECT date_part('epoch', toDateTime('2024-03-17 00:00:00', 'UTC')) = EXTRACT(EPOCH FROM toDateTime('2024-03-17 00:00:00', 'UTC'));
