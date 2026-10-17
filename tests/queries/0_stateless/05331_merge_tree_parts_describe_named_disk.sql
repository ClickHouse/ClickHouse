-- The disk of `mergeTreeParts` is query-local, so its description cannot have a `name`. `DESCRIBE`
-- only parses the arguments and never creates the disk, so the `name` has to be rejected while the
-- arguments are parsed, otherwise `DESCRIBE` would accept what every read rejects.

DESCRIBE mergeTreeParts(
    structure('x UInt8'),
    parts(),
    disk(name = 'mtp_describe_named', type = local, path = '/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

DESCRIBE mergeTreeParts(
    structure('x UInt8'),
    parts(),
    disk(type = local, path = '/', name = 'mtp_describe_named'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(),
    disk(name = 'mtp_describe_named', type = local, path = '/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }
