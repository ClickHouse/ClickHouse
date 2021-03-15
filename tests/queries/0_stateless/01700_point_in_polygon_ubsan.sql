SELECT pointInPolygon((0, 0), [[(0, 0), (10, 10), (256, -9223372036854775808)]]) FORMAT Null ;-- { serverError 36 }
