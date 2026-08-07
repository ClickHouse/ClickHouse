select toDecimal128(42.42, 5) as d format JSONEachRow settings output_format_json_quote_decimals=1;
insert into function file(02421_data.jsonl) select '42.42' as d settings engine_file_truncate_on_insert=1;
select * from file(02421_data.jsonl, auto, 'd Decimal32(3)');

