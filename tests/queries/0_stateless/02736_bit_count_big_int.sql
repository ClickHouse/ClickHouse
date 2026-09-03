SELECT bitCount(CAST(-1 AS UInt128));
SELECT bitCount(CAST(-1 AS UInt256));

SELECT bitCount(CAST(-1 AS Int128));
SELECT bitCount(CAST(-1 AS Int256));

SELECT bitCount(CAST(-1 AS UInt128) - 1);
SELECT bitCount(CAST(-1 AS UInt256) - 2);

SELECT bitCount(CAST(-1 AS Int128) - 3);
SELECT bitCount(CAST(-1 AS Int256) - 4);

SELECT bitCount(CAST(0xFFFFFFFFFFFFFFFF AS Int256));

SELECT toTypeName(bitCount(1::UInt128));
SELECT toTypeName(bitCount(1::UInt256));

SELECT toTypeName(bitCount(1::Int128));
SELECT toTypeName(bitCount(1::Int256));
