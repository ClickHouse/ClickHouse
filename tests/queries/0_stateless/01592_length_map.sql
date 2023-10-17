set allow_experimental_map_type = 1;

select length(map(1,2,3,4));
select length(map());
select empty(map(1,2,3,4));
select empty(map());
select notEmpty(map(1,2,3,4));
select notEmpty(map());
