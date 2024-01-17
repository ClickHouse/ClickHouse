select * from s3('http://localhost:11111/test/a.tsv', '\ninjection\n', 'admin'); -- { serverError 36 }
select * from deltaLake('http://localhost:11111/test/a.tsv', '\ninjection\n', 'admin'); -- { serverError 36 }
select * from hudi('http://localhost:11111/test/a.tsv', '\ninjection\n', 'admin'); -- { serverError 36 }
select * from iceberg('http://localhost:11111/test/a.tsv', '\ninjection\n', 'admin'); -- { serverError 36 }
