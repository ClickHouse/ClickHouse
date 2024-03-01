create table a (x Int64) engine URL('https://example.com/', CSV, headers('foo' = 'bar'));
show create a;
