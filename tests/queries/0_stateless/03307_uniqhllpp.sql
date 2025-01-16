-- { echoOn }
with arrayJoin([0, 1, 2, 10]) as x select uniqHLLPP(0.05)(cityHash64(x));
with arrayJoin([0, 6, 7, 9, 10]) as x select uniqHLLPP(0.05)(cityHash64(x));

with number + 1 as col 
select 
    count(col), 
    count(distinct col), 
    uniqHLLPP(0.00001)(cityHash64(col)),
    uniqHLLPP(0.0001)(cityHash64(col)),
    uniqHLLPP(0.001)(cityHash64(col)),
    uniqHLLPP(0.01)(cityHash64(col)), 
    uniqHLLPP(0.02)(cityHash64(col)), 
    uniqHLLPP(0.03)(cityHash64(col)), 
    uniqHLLPP(0.04)(cityHash64(col)),
    uniqHLLPP(0.05)(cityHash64(col)),
    uniqHLLPP(cityHash64(col)),
    uniqHLLPP()(cityHash64(col))
from numbers(10000);

SELECT uniqHLLPPMerge(0.05)(x)
FROM
(
    SELECT uniqHLLPPState(0.05)(cityHash64(number + 1)) AS x
    FROM numbers(49999)
);

select uniqHLLPP(cityHash64(number)) from numbers(10);
select uniqHLLPP(0.05)(cityHash64(number)) from numbers(10);

select uniqHLLPP('abc', 12)(cityHash64(number)) from numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select uniqHLLPP('abc')(cityHash64(number)) from numbers(10); -- { serverError CANNOT_CONVERT_TYPE }
select uniqHLLPP(12)(cityHash64(number)) from numbers(10); -- { serverError PARAMETER_OUT_OF_BOUND }
select uniqHLLPP(-12)(cityHash64(number)) from numbers(10); -- { serverError PARAMETER_OUT_OF_BOUND }
select uniqHLLPP(0)(cityHash64(number)) from numbers(10); -- { serverError PARAMETER_OUT_OF_BOUND }
select uniqHLLPP(0.000000000000000000001)(cityHash64(number)) from numbers(100); -- { serverError PARAMETER_OUT_OF_BOUND }
-- { echoOff }
