select hex((SELECT partitionByHyperplanes([toFloat32(2.0), toFloat32(3.0)], [toFloat32(1.0), toFloat32(1.0)])));
select hex((SELECT partitionByHyperplanes([toFloat32(2.0), toFloat32(3.0)], [toFloat32(1.0), toFloat32(-1.0)])));
SELECT hex((select partitionByHyperplanes([toFloat32(2.0), toFloat32(3.0)], [[toFloat32(1.0), toFloat32(1.0)], [toFloat32(1.0), toFloat32(2.0)]])));

SELECT hex((select partitionByHyperplanes([toFloat32(2.0), toFloat32(3.0)], [[toFloat32(1.0), toFloat32(1.0)], [toFloat32(1.0)]])));
