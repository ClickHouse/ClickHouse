SELECT greatCircleAngle(1048575, 257, -9223372036854775808, 1048576) - NULL, bar(7, -inf, 1024); -- { serverError 36 } 
