-- https://github.com/ClickHouse/ClickHouse/issues/70569
-- Reproduces UBSAN alert about misaligned address
SELECT anyLast(id), anyLast(time), exponentialTimeDecayedAvg(10)(id, time) FROM values('id Int8, time DateTime', (1,1),(1,2),(2,3),(3,3),(3,5)); -- { serverError BAD_ARGUMENTS }
