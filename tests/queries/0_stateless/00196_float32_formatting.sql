SELECT 21.99;
SELECT toFloat32(21.99);
SELECT visibleWidth(21.99);
SELECT visibleWidth(toFloat32(21.99));
SELECT materialize(21.99);
SELECT toFloat32(materialize(21.99));
SELECT visibleWidth(materialize(21.99));
SELECT visibleWidth(toFloat32(materialize(21.99)));
