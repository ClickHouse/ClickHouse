SET enable_analyzer=1;
CREATE TABLE testdata (`1` String) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO testdata VALUES ('testdata');

SELECT *
FROM (
   SELECT if(isValidUTF8(`1`), NULL, 'error!') AS error_message,
          if(error_message IS NULL, 1, 0) AS valid
     FROM testdata
)
WHERE valid;

select * from (select 'str' as `1`) where 1;
