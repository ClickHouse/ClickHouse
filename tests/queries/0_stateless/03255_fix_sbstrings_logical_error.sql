SELECT 
    concat(concat(11),
    5, 
    countSubstringsCaseInsensitive(
        concat(countSubstringsCaseInsensitive(
            concat(11, toString(number), materialize('aaa111'), 6, materialize(6)), char(number)), 
            'aaa111'), 
        char(countSubstringsCaseInsensitive(concat(' test'), char(toLowCardinality(6))))), 
    'aaa111', 6) FROM numbers(1);
