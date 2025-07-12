CREATE AGGREGATE FUNCTION my_agg(
    init_state Tuple(Int64, UInt64),
    process_func Tuple(Int64, UInt64), Int64 -> Tuple(Int64, UInt64),
    finalize_func Tuple(Int64, UInt64) -> Int64,
    merge_func Tuple(Int64, UInt64), Tuple(Int64, UInt64) -> Tuple(Int64, UInt64)
) AS 
(
    (0, 0),
    (state, value) -> (state.1 + value, state.2 + 1),
    state -> state.1 / state.2,
    (state1, state2) -> (state1.1 + state2.1, state1.2 + state2.2)
);

SELECT 