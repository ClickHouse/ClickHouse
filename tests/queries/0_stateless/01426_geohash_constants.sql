SELECT geohashesInBox(1., 2., 3., 4., 1);
SELECT geohashesInBox(materialize(1.), 2., 3., 4., 2);
SELECT geohashesInBox(1., materialize(2.), 3., 4., 3);
SELECT geohashesInBox(1., 2., materialize(3.), 4., 1);
SELECT geohashesInBox(1., 2., 3., materialize(4.), 2);
SELECT geohashesInBox(1., 2., 3., 4., materialize(3));
