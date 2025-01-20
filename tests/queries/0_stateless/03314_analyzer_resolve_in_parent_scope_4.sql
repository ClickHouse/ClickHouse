with
    arrayMap(x -> x + 1, [0]) as a
select
    1
where
    1 in (select arrayJoin(a))
settings allow_experimental_analyzer = 1;
