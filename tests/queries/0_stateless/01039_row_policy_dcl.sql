SHOW POLICIES;
CREATE POLICY p1 ON dummytable; -- { serverError 497 }
