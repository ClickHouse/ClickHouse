set output_format_write_statistics=0;
set max_threads=1;

select groupFormat('JSONEachRow')(number, toString(number)) from numbers(3);

set format_csv_delimiter=';';
select groupFormat('CSVWithNamesAndTypes')(number, toString(number)) from numbers(2);

select groupFormat('JSONCompactColumns')(number, toString(number)) from numbers(2);

set output_format_json_quote_64bit_integers=1;
select groupFormat('JSONEachRow')(number) from numbers(2);
set output_format_json_quote_64bit_integers=0;

select groupFormat('JSONEachRow')(if(number = 0, NULL, number)) from numbers(2);

select groupFormat('JSONEachRow')(number) from (select number from numbers(3) order by number desc);

select key, groupFormat('JSONEachRow')(number)
from (select number, number % 2 as key from numbers(4) order by number)
group by key
order by key;

select groupFormat('JSONEachRow')(number) from numbers(0);

select groupFormat(123)(number) from numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select groupFormat() from numbers(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
