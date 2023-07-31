-- Tags: no-fasttest

insert into function file('02268_data.jsonl', 'TSV') select 1;
select * from file('02268_data.jsonl'); --{serverError 117}

insert into function file('02268_data.jsonCompactEachRow', 'TSV') select 1;
select * from file('02268_data.jsonCompactEachRow'); --{serverError 117}
