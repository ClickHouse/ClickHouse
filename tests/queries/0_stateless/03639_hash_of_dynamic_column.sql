select sipHash64(42::Dynamic);
select sipHash64(42::Dynamic(max_types=0));

select sipHash64(43::Dynamic);
select sipHash64(43::Dynamic(max_types=0));

select sipHash64('str1'::Dynamic);
select sipHash64('str1'::Dynamic(max_types=0));

select sipHash64('str2'::Dynamic);
select sipHash64('str2'::Dynamic(max_types=0));

select sipHash64(NULL::Dynamic);
select sipHash64(NULL::Dynamic(max_types=0));

select sipHash64(tuple(42)::Dynamic);
select sipHash64(tuple(42)::Dynamic(max_types=0));
