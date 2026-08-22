select * from format(CSV, 'd Date, s String', 'abcdefgh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '2bcdefgh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '20cdefgh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '202defgh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '2020efgh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '20200fgh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '202001gh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '2020010h,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '20200102,SomeString');
select * from format(CSV, 'd Date, s String', 'abcd-ef-gh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '2bcd-ef-gh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '20cd-ef-gh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '202d-ef-gh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '2020-ef-gh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '2020-f-gh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '2020-f-g,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '2020-0f-gh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '2020-01-gh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '2020-01-h,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '2020-1-gh,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '2020-1-h,SomeString'); -- {serverError CANNOT_PARSE_DATE}
select * from format(CSV, 'd Date, s String', '2020-01-02,SomeString');
select * from format(CSV, 'd Date, s String', '2020-01-2,SomeString');
select * from format(CSV, 'd Date, s String', '2020-1-2,SomeString');
select * from format(CSV, 'd Date, s String', '2020-1-02,SomeString');
