-- { echo }
-- Another test for window functions because the other one is too long.

-- some craziness with a mix of materialized and unmaterialized const columns
-- after merging sorted transform, that used to break the peer group detection in
-- the window transform.
CREATE TABLE order_by_const
(
    `a` UInt64,
    `b` UInt64,
    `c` UInt64,
    `d` UInt64
)
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 8192;

truncate table order_by_const;
system stop merges order_by_const;
INSERT INTO order_by_const(a, b, c, d) VALUES (1, 1, 101, 1), (1, 2, 102, 1), (1, 3, 103, 1), (1, 4, 104, 1);
INSERT INTO order_by_const(a, b, c, d) VALUES (1, 5, 104, 1), (1, 6, 105, 1), (2, 1, 106, 2), (2, 1, 107, 2);
INSERT INTO order_by_const(a, b, c, d) VALUES (2, 2, 107, 2), (2, 3, 108, 2), (2, 4, 109, 2);
SELECT row_number() OVER (order by 1, a) FROM order_by_const;

drop table order_by_const;

-- expressions in window frame
select count() over (rows between 1 + 1 preceding and 1 + 1 following) from numbers(10);

-- signed and unsigned in offset do not cause logical error
select count() over (rows between 2 following and 1 + -1 following) FROM numbers(10); -- { serverError 36 }

-- default arguments of lagInFrame can be a subtype of the argument
select number,
    lagInFrame(toNullable(number), 2, null) over w,
    lagInFrame(number, 2, 1) over w
from numbers(10)
window w as (order by number)
;

-- the case when current_row goes past the partition end at the block end
select number, row_number() over (partition by number rows between unbounded preceding and 1 preceding) from numbers(4) settings max_block_size = 2;
