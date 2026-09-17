SELECT 'wide integer order';

SELECT isNull(product(x))
FROM
(
    SELECT if(number = 5, toInt256(0), toInt256('10000000000000000000000000000000000000000000000000000000000000000000000000000')) AS x
    FROM numbers(6)
)
SETTINGS max_threads = 1, max_block_size = 6;

SELECT product(x)
FROM
(
    SELECT if(number = 0, toInt256(0), toInt256('10000000000000000000000000000000000000000000000000000000000000000000000000000')) AS x
    FROM numbers(6)
)
SETTINGS max_threads = 1, max_block_size = 6;
