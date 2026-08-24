SELECT CASE 1 WHEN 1 THEN 2 END;

SELECT id,
    CASE id
         WHEN 1 THEN 'Z'
    END x
FROM  (SELECT 1 as id);

SELECT id,
       CASE id
            WHEN 1 THEN 'Z'
            ELSE 'X'
     END x
FROM  (SELECT 1 as id);
