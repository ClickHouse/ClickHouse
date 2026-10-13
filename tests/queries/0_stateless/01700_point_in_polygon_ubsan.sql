SELECT pointInPolygon((0, 0), [[(0, 0), (10, 10), (256, -9223372036854775808)]]) FORMAT Null ;-- { serverError BAD_ARGUMENTS }

-- An unbounded bounding box has no usable grid.
SELECT pointInPolygon((1e308, 1.), [(-1.7976931348623157e308, 0.), (1.7976931348623157e308, 0.), (0., 2.)]) FORMAT Null; -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((1e308, 1.), [[[(-1.7976931348623157e308, 0.), (1.7976931348623157e308, 0.), (0., 2.)]]]) FORMAT Null; -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((1e308, 1.), [[(-1.7976931348623157e308, 0.), (1.7976931348623157e308, 0.), (0., 2.)], [(0.1, 0.1), (0.2, 0.1), (0.15, 0.2)]]) FORMAT Null; -- { serverError BAD_ARGUMENTS }

SET validate_polygons = 0;
SELECT pointInPolygon((1., 1e308), [(0., -1.7976931348623157e308), (1., 1.7976931348623157e308), (2., 0.)]) FORMAT Null; -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((1., 0.1), [(inf, 0.), (inf, 0.1), (inf, 0.2)]) FORMAT Null; -- { serverError BAD_ARGUMENTS }
SET validate_polygons = 1;

-- A finite bounding box stays usable up to a coordinate of 1e150 in absolute value.
SELECT pointInPolygon((0., 1.), [(-1e150, 0.), (1e150, 0.), (0., 2.)]);
SELECT pointInPolygon((1e149, 0.5), [(-1e150, 0.), (1e150, 0.), (0., 2.)]);
SELECT pointInPolygon((1e197, 1.99), [(-1e200, 0.), (1e200, 0.), (0., 2.)]); -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((3., 3.), [(0., 0.), (10., 0.), (10., 10.), (0., 10.)]);
SELECT pointInPolygon((20., 3.), [(0., 0.), (10., 0.), (10., 10.), (0., 10.)]);
