-- Test that accurateCastOrDefault preserves Const column type (issue #100066)
WITH '019d0101-e8eb-7173-8a68-54c8b703e001' AS uuid
SELECT
    toColumnTypeName(accurateCast(uuid, 'UUID')),
    toColumnTypeName(accurateCastOrNull(uuid, 'UUID')),
    toColumnTypeName(accurateCastOrDefault(uuid, 'UUID'));
