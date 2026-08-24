-- { echoOn }

SET allow_experimental_nullable_tuple_type = 1;

SET enable_analyzer = 1;

SELECT CAST(NULL AS Nullable(Tuple(UInt32, UInt32))) IN (materialize(NULL), materialize(NULL)), CAST(NULL AS Nullable(Tuple(UInt32, UInt32))) NOT IN (materialize(NULL), materialize(NULL)) SETTINGS transform_null_in = 1;
SELECT tuple(NULL, NULL)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (materialize(NULL), materialize(NULL)), tuple(NULL, NULL)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) NOT IN (materialize(NULL), materialize(NULL)) SETTINGS transform_null_in = 1;
SELECT tuple(NULL, NULL) IN (materialize(NULL), materialize(NULL)), tuple(NULL, NULL) NOT IN (materialize(NULL), materialize(NULL)) SETTINGS transform_null_in = 1;
SELECT tuple(NULL, 42)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (materialize(NULL), materialize(42)), tuple(NULL, 42)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) NOT IN (materialize(NULL), materialize(42)) SETTINGS transform_null_in = 1;
SELECT nullIn(CAST(NULL AS Nullable(Tuple(UInt32, UInt32))), (materialize(NULL), materialize(NULL))), notNullIn(CAST(NULL AS Nullable(Tuple(UInt32, UInt32))), (materialize(NULL), materialize(NULL)));
SELECT CAST(NULL AS Nullable(Tuple(UInt32, UInt32))) IN (materialize(NULL), materialize(NULL)), CAST(NULL AS Nullable(Tuple(UInt32, UInt32))) NOT IN (materialize(NULL), materialize(NULL)) SETTINGS transform_null_in = 0;
