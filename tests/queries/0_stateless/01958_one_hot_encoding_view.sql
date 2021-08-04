DROP TABLE IF EXISTS one_hot_source;
CREATE TABLE one_hot_source (Id String, X String, Y String, Z String) Engine=Memory;
INSERT INTO TABLE one_hot_source VALUES (1, 'A','','c'),(2,'B','a','Y'),(3,'C','','c'),(4,'A','b','c'),(5,'D','c','d'),(6,'B','d','c');
-- use one_hot_encoding_view to encode X,Y,Z, and Id columns
SELECT * FROM one_hot_encoding_view((SELECT * FROM one_hot_source),X,Y,Z,Id);
DROP TABLE one_hot_source;
