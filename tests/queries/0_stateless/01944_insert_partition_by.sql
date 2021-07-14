INSERT INTO TABLE FUNCTION file('foo.csv', 'CSV', 'id Int32, val Int32') PARTITION BY val VALUES (1, 1), (2, 2); -- { serverError 48 }
