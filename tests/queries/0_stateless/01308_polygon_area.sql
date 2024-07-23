select polygonAreaCartesian([[[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]]]);
select round(polygonAreaSpherical([[[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]]]), 14);
SELECT polygonAreaCartesian([]); -- { serverError 36 }
