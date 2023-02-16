SELECT * FROM generateRandom('i8', 1, 10, 10); -- { serverError 62 }
SELECT * FROM generateRandom; -- { serverError 60 }
SELECT * FROM generateRandom(); -- { serverError 42 }
SELECT * FROM generateRandom('i8 UInt8', 1, 10, 10, 10, 10); -- { serverError 42 }
SELECT * FROM generateRandom('', 1, 10, 10); -- { serverError 62 }
