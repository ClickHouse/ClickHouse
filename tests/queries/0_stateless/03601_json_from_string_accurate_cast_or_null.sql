select accurateCastOrNull('{"a" : 42, "a" : 43}', 'JSON');
select accurateCastOrNull(materialize('{"a" : 42, "a" : 43}'), 'JSON');
select accurateCastOrDefault('{"a" : 42, "a" : 43}', 'JSON');
select accurateCastOrDefault(materialize('{"a" : 42, "a" : 43}'), 'JSON');

