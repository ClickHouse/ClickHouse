-- create table
DROP TABLE IF EXISTS simple;
CREATE TABLE simple(k UInt64,s String,INDEX af (s) TYPE gin(2) GRANULARITY 1) ENGINE = MergeTree() ORDER BY k
SETTINGS index_granularity = 2;
-- insert test data into table
INSERT INTO simple VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'),(104, 'Dlick a04'),(105, 'Elick a05'),(106, 'Alick a06'),(107, 'Blick a07'),(108, 'Click a08'),(109, 'Dlick a09'),(110, 'Elick b10'),(111, 'Alick b01'),(112, 'Blick b02'),(113, 'Click b03'),(114, 'Dlick b04'),(115, 'Elick b05'),(116, 'Alick b06'),(117, 'Blick b07'),(118, 'Click b08'),(119, 'Dlick b09'),(120, 'Elick b10');
-- check gin index was created
SELECT name, type FROM system.data_skipping_indices where (table =='simple') limit 1;
-- search gin index
SELECT * FROM simple WHERE s LIKE '%01%';