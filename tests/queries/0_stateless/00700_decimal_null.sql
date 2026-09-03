DROP TABLE IF EXISTS decimal;

CREATE TABLE IF NOT EXISTS decimal
(
    a DEC(9, 2),
    b DEC(18, 5),
    c DEC(38, 5),
    d Nullable(DEC(9, 4)),
    e Nullable(DEC(18, 8)),
    f Nullable(DEC(38, 8))
) ENGINE = Memory;

SELECT toNullable(toDecimal32(32, 0)) AS x, assumeNotNull(x);
SELECT toNullable(toDecimal64(64, 0)) AS x, assumeNotNull(x);
SELECT toNullable(toDecimal128(128, 0)) AS x, assumeNotNull(x);

SELECT ifNull(toDecimal32(1, 0), NULL), ifNull(toDecimal64(1, 0), NULL), ifNull(toDecimal128(1, 0), NULL);
SELECT ifNull(toNullable(toDecimal32(2, 0)), NULL), ifNull(toNullable(toDecimal64(2, 0)), NULL), ifNull(toNullable(toDecimal128(2, 0)), NULL);
SELECT ifNull(NULL, toDecimal32(3, 0)), ifNull(NULL, toDecimal64(3, 0)), ifNull(NULL, toDecimal128(3, 0));
SELECT ifNull(NULL, toNullable(toDecimal32(4, 0))), ifNull(NULL, toNullable(toDecimal64(4, 0))), ifNull(NULL, toNullable(toDecimal128(4, 0)));

SELECT coalesce(toDecimal32(5, 0), NULL), coalesce(toDecimal64(5, 0), NULL), coalesce(toDecimal128(5, 0), NULL);
SELECT coalesce(NULL, toDecimal32(6, 0)), coalesce(NULL, toDecimal64(6, 0)), coalesce(NULL, toDecimal128(6, 0));

SELECT coalesce(toNullable(toDecimal32(7, 0)), NULL), coalesce(toNullable(toDecimal64(7, 0)), NULL), coalesce(toNullable(toDecimal128(7, 0)), NULL);
SELECT coalesce(NULL, toNullable(toDecimal32(8, 0))), coalesce(NULL, toNullable(toDecimal64(8, 0))), coalesce(NULL, toNullable(toDecimal128(8, 0)));

SELECT nullIf(toNullable(toDecimal32(1, 0)), toDecimal32(1, 0)), nullIf(toNullable(toDecimal64(1, 0)), toDecimal64(1, 0));
SELECT nullIf(toDecimal32(1, 0), toNullable(toDecimal32(1, 0))), nullIf(toDecimal64(1, 0), toNullable(toDecimal64(1, 0)));
SELECT nullIf(toNullable(toDecimal32(1, 0)), toDecimal32(2, 0)), nullIf(toNullable(toDecimal64(1, 0)), toDecimal64(2, 0));
SELECT nullIf(toDecimal32(1, 0), toNullable(toDecimal32(2, 0))), nullIf(toDecimal64(1, 0), toNullable(toDecimal64(2, 0)));
SELECT nullIf(toNullable(toDecimal128(1, 0)), toDecimal128(1, 0));
SELECT nullIf(toDecimal128(1, 0), toNullable(toDecimal128(1, 0)));
SELECT nullIf(toNullable(toDecimal128(1, 0)), toDecimal128(2, 0));
SELECT nullIf(toDecimal128(1, 0), toNullable(toDecimal128(2, 0)));

INSERT INTO decimal (a, b, c, d, e, f) VALUES (1.1, 1.1, 1.1, 1.1, 1.1, 1.1);
INSERT INTO decimal (a, b, c, d) VALUES (2.2, 2.2, 2.2, 2.2);
INSERT INTO decimal (a, b, c, e) VALUES (3.3, 3.3, 3.3, 3.3);
INSERT INTO decimal (a, b, c, f) VALUES (4.4, 4.4, 4.4, 4.4);
INSERT INTO decimal (a, b, c) VALUES (5.5, 5.5, 5.5);

SELECT * FROM decimal ORDER BY d, e, f;
SELECT isNull(a), isNotNull(a) FROM decimal WHERE a = toDecimal32(5.5, 1);
SELECT isNull(b), isNotNull(b) FROM decimal WHERE a = toDecimal32(5.5, 1);
SELECT isNull(c), isNotNull(c) FROM decimal WHERE a = toDecimal32(5.5, 1);
SELECT isNull(d), isNotNull(d) FROM decimal WHERE a = toDecimal32(5.5, 1);
SELECT isNull(e), isNotNull(e) FROM decimal WHERE a = toDecimal32(5.5, 1);
SELECT isNull(f), isNotNull(f) FROM decimal WHERE a = toDecimal32(5.5, 1);
SELECT count() FROM decimal WHERE a IS NOT NULL;
SELECT count() FROM decimal WHERE b IS NOT NULL;
SELECT count() FROM decimal WHERE c IS NOT NULL;
SELECT count() FROM decimal WHERE d IS NULL;
SELECT count() FROM decimal WHERE e IS NULL;
SELECT count() FROM decimal WHERE f IS NULL;
SELECT count() FROM decimal WHERE d IS NULL AND e IS NULL;
SELECT count() FROM decimal WHERE d IS NULL AND f IS NULL;
SELECT count() FROM decimal WHERE e IS NULL AND f IS NULL;

DROP TABLE IF EXISTS decimal;
