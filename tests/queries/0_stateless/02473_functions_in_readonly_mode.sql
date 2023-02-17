SELECT * from numbers(1);
SELECT * from format('TSV', '123');
SELECT * from numbers(1) SETTINGS readonly=1;
SELECT * from format('TSV', '123') SETTINGS readonly=1; -- { serverError READONLY }