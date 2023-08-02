# We support specifying aliases in any place in the query, including CASE expression:

with arrayJoin([1,2]) as arg
select arg,
       (case
           when arg = 1
           then 1 as one
           when arg = 2
           then one / 2
       end) as imposible;
