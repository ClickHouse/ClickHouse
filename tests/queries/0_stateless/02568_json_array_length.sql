-- { echoOn }
select JSONArrayLength(null);
select JSONArrayLength('');
select JSONArrayLength('[]');
select JSONArrayLength('[1,2,3]');
select JSONArrayLength('[[1,2],[5,6,7]]');
select JSONArrayLength('[{"a":123},{"b":"hello"}]');
select JSONArrayLength('[1,2,3,[33,44],{"key":[2,3,4]}]');
select JSONArrayLength('{"key":"not a json array"}');
select JSONArrayLength('[1,2,3,4,5');

select JSON_ARRAY_LENGTH('[1,2,3,4,5');
select JSON_ARRAY_LENGTH('[1,2,3,4,5]');

select JSONArrayLength(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select JSONArrayLength(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
