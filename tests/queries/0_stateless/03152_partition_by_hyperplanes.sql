select hex((SELECT partitionByHyperplanes([toFloat32(2.0), toFloat32(3.0)], [toFloat32(1.0), toFloat32(1.0)])));
select hex((SELECT partitionByHyperplanes([toFloat32(2.0), toFloat32(3.0)], [toFloat32(1.0), toFloat32(-1.0)])));
SELECT hex((select partitionByHyperplanes([toFloat32(2.0), toFloat32(3.0)], [[toFloat32(1.0), toFloat32(1.0)], [toFloat32(1.0), toFloat32(2.0)]])));
select hex((SELECT partitionByHyperplanes((select range(1024) :: Array(Float32)), (select range(1024) :: Array(Float32)))));
select hex((SELECT partitionByHyperplanes((select range(1024) :: Array(Float32)), (select range(-1024, 0) :: Array(Float32)))));
select hex((SELECT partitionByHyperplanes((select range(1024) :: Array(Float32)), (select arrayWithConstant(5, (select range(1024) :: Array(Float32)))))));

SELECT hex((select partitionByHyperplanes([toFloat32(2.0), toFloat32(3.0)], [[toFloat32(1.0), toFloat32(1.0)], [toFloat32(1.0)]])));  -- { serverError 190 }
SELECT hex((select partitionByHyperplanes([toFloat32(2.0), toFloat32(3.0)], []))); -- { serverError 44 }
SELECT hex((select partitionByHyperplanes([toFloat32(2.0), toFloat32(3.0)], [[[toFloat32(2.0), toFloat32(3.0)]]]))); -- { serverError 44 }
