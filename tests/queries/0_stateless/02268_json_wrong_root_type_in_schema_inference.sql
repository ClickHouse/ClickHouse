insert into function file('02268_data.jsonl', 'TSV') select 1;
select * from file('02268_data.jsonl'); --{serverError CANNOT_EXTRACT_TABLE_STRUCTURE}

insert into function file('02268_data.jsonCompactEachRow', 'TSV') select 1;
select * from file('02268_data.jsonCompactEachRow'); --{serverError CANNOT_EXTRACT_TABLE_STRUCTURE}

