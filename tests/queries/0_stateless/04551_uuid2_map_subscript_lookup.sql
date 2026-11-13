-- `mk[key]` (Map subscript) lookups must reconcile `UUID`/`UUID2` layouts, mirroring what `has`/`mapContains`
-- already do (see 04542_uuid2_collection_constant_conversion.sql). Two distinct code paths previously
-- compared the raw representations directly instead of converting first:
--   * a constant key is rewritten by `FunctionToSubcolumnsPass` into a `map.key_<serialized>` subcolumn
--     access at analysis time; the constant was inserted into a column of the map's key type without
--     converting its layout first, so the serialized subcolumn name never matched any real key.
--   * a non-constant key reaches `FunctionArrayElement::executeMap`, which compared the raw
--     representations directly.
-- In both cases the lookup silently returned the default value instead of the actual one.

DROP TABLE IF EXISTS t_uuid2_map_subscript;
CREATE TABLE t_uuid2_map_subscript (id UInt64, mk Map(UUID2, UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_uuid2_map_subscript SELECT number, map(toUUID2(concat('61f0c404-5cb3-11e7-907b-a6006ad3db0', toString(number))), number) FROM numbers(4);

SELECT '-- constant key of the other UUID flavor (FunctionToSubcolumnsPass rewrite)';
SELECT id, mk[toUUID('61f0c404-5cb3-11e7-907b-a6006ad3db02')] FROM t_uuid2_map_subscript ORDER BY id;

SELECT '-- non-constant key of the other UUID flavor (FunctionArrayElement::executeMap)';
SELECT id, mk[materialize(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3db02'))] FROM t_uuid2_map_subscript ORDER BY id;

SELECT '-- matching flavor still works (sanity)';
SELECT id, mk[toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3db02')] FROM t_uuid2_map_subscript ORDER BY id;

DROP TABLE t_uuid2_map_subscript;
