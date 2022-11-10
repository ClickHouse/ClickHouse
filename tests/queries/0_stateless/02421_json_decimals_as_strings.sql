select toDecimal128(42.42, 5) as d format JSONEachRow settings output_format_json_quote_decimals=1;
insert into function file(data02421.jsonl) select '42.42' as d settings engine_file_truncate_on_insert=1;
select * from file(data02421.jsonl, auto, 'd Decimal32(3)');

