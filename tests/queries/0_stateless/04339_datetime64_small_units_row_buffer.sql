SET date_time_input_format = 'basic';
SELECT a FROM format(TSV, 'a DateTime64(2, \'UTC\')',
$$3333.77
99.1
1234.5
23.9
$$) ORDER BY a;
SELECT a FROM format(TSV, 'a DateTime64(0, \'UTC\')',
$$2025-08-31 13:45:30
2025.08.31
2020-01-02 03:04:05
$$) ORDER BY a;
