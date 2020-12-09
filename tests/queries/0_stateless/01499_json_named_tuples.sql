create table named_tuples engine File(JSONEachRow)
    settings output_format_json_named_tuples_as_objects = 1
    as select cast(tuple(number, number * 2), 'Tuple(a int, b int)') c
        from numbers(3);

select * from named_tuples format JSONEachRow settings output_format_json_named_tuples_as_objects = 1;
