set allow_experimental_dynamic_type = 1;

select 'Hello'::Dynamic(max_types=1) as d format PrettyJSONEachRow;
select 'Hello'::Dynamic(max_types=0) as d format PrettyJSONEachRow;


