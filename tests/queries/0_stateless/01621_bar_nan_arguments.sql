SELECT bar((greatCircleAngle(65537, 2, 1, 1) - 1) * 65535, 1048576, 1048577, nan); -- { serverError 43 }
select bar(1,1,1,nan); -- { serverError 43 }
