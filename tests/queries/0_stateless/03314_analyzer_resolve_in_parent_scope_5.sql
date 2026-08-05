with query_1 as (
    with shared_data as (
        select 1 as value
    ), shared_data_2 as (
        select * from shared_data
    )
    select * from shared_data_2
), shared_data as (
    select * from query_1
)
select * from shared_data s;
