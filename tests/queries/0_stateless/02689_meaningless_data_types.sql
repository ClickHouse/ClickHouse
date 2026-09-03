SELECT 0::Bool(Upyachka); -- { serverError DATA_TYPE_CANNOT_HAVE_ARGUMENTS }
SELECT [(1, 2), (3, 4)]::Ring(Upyachka); -- { serverError DATA_TYPE_CANNOT_HAVE_ARGUMENTS }
SELECT '1.1.1.1'::IPv4('Hello, world!'); -- { serverError DATA_TYPE_CANNOT_HAVE_ARGUMENTS }
