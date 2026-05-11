SELECT L1Norm(materialize([1.0, -2.0, 3.0, -4.0]::Array(Float32)));
SELECT L2Norm(materialize([1.0, 2.0, 2.0, 4.0]::Array(Float32)));
SELECT LinfNorm(materialize([1.0, -7.0, 3.0, -4.0]::Array(Float32)));
SELECT L2Distance(materialize([1.0, 2.0, 3.0, 4.0]::Array(Float32)), materialize([0.0, 1.0, 2.0, 3.0]::Array(Float32)));
SELECT L2Distance(materialize(range(16))::Array(Float32), materialize(range(16))::Array(Float32));
SELECT L2Distance(materialize(range(17))::Array(Float32), materialize(range(17))::Array(Float32));
WITH range(8)::Array(Float32) AS v SELECT L2Distance(v, materialize(range(8))::Array(Float32));
WITH range(9)::Array(Float32) AS v SELECT L2Distance(v, materialize(range(9))::Array(Float32));
WITH range(16)::Array(Float64) AS v SELECT L2Distance(v, materialize(range(16))::Array(Float64));
