-- { echoOn }
select json_array_length(null);
select json_array_length('');
select json_array_length('[]');
select json_array_length('[1,2,3]');
select json_array_length('[[1,2],[5,6,7]]');
select json_array_length('[{"a":123},{"b":"hello"}]');
select json_array_length('[1,2,3,[33,44],{"key":[2,3,4]}]');
select json_array_length('{"key":"not a json array"}');
select json_array_length('[1,2,3,4,5');

select json_array_length(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select json_array_length(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
