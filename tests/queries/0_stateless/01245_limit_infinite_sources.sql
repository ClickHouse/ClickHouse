SELECT number
FROM 
(
    SELECT zero AS number
    FROM remote('127.0.0.2', system.zeros)
    UNION ALL
    SELECT number + sleep(0.5)
    FROM system.numbers
)
WHERE number = 1
LIMIT 1
