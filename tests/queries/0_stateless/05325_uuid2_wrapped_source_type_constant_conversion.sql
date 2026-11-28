-- `UUID` and `UUID2` share the `Field` representation but differ in the order of the two 64-bit halves, so
-- converting a constant between them must swap the halves even when the source type is wrapped into
-- `Nullable` or `LowCardinality`, both for a scalar and for the elements of a collection.

SELECT 'nullable', '61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2 IN (toNullable(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')));
SELECT 'low cardinality', '61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2 IN (toLowCardinality(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')));
SELECT 'reverse', toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') IN (toNullable('61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2));
SELECT 'array', '61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2 IN [toNullable(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))];
SELECT 'different value', '61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2 IN (toNullable(toUUID('71f0c404-5cb3-11e7-907b-a6006ad3dba0')));
