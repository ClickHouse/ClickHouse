DROP TABLE IF EXISTS line_as_string;
CREATE TABLE line_as_string (field String) ENGINE = Memory;
INSERT INTO line_as_string FORMAT LineAsString "I love apple","I love banana","I love pear";
SELECT * FROM line_as_string;
DROP TABLE line_as_string;
