-- `Bool` is a custom-named `UInt8`, so `IDataType::equals` treats `Bool` and `UInt8` (and wrapper pairs
-- such as `Nullable(Bool)` and `Nullable(UInt8)`) as the same type. The constant conversion must still
-- normalize a raw `Bool` byte (2 here, produced by `reinterpret`) to the semantic `true` rather than
-- alias it byte-for-byte.
SELECT * FROM values('x UInt8', reinterpret(toUInt8(2), 'Bool'));
SELECT * FROM values('x Nullable(UInt8)', toNullable(reinterpret(toUInt8(2), 'Bool')));
SELECT * FROM values('x Array(UInt8)', [reinterpret(toUInt8(2), 'Bool')]);
SELECT * FROM values('x Tuple(UInt8)', tuple(reinterpret(toUInt8(2), 'Bool')));
SELECT * FROM values('x Map(String, UInt8)', map('a', reinterpret(toUInt8(2), 'Bool')));

-- The `IN` set builder converts each literal strictly: a `Bool` literal is normalized to `true` before
-- it is compared with a `UInt8`, and 2 is not a valid `Bool`.
SELECT toUInt8(2) IN (reinterpret(toUInt8(2), 'Bool'));
SELECT toUInt8(1) IN (reinterpret(toUInt8(2), 'Bool'));
SELECT reinterpret(toUInt8(2), 'Bool') IN (toUInt8(2));
SELECT reinterpret(toUInt8(2), 'Bool') IN (toUInt8(1));

-- Identical types containing `Bool` are not a byte-for-byte no-op either: the raw byte must still be
-- normalized, as `CAST` does for `Bool -> Bool`.
SELECT x, x = true FROM values('x Bool', reinterpret(toUInt8(2), 'Bool'));
SELECT x, x = true FROM values('x Nullable(Bool)', toNullable(reinterpret(toUInt8(2), 'Bool')));
SELECT x, x = [true] FROM values('x Array(Bool)', [reinterpret(toUInt8(2), 'Bool')]);
SELECT x, x = tuple(true) FROM values('x Tuple(Bool)', tuple(reinterpret(toUInt8(2), 'Bool')));
SELECT x, x = map('a', true) FROM values('x Map(String, Bool)', map('a', reinterpret(toUInt8(2), 'Bool')));
SELECT true IN (reinterpret(toUInt8(2), 'Bool'));
SELECT reinterpret(toUInt8(3), 'Bool') IN (reinterpret(toUInt8(2), 'Bool'));
