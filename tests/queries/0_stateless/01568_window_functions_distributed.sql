-- { echo }
set allow_experimental_window_functions = 1;

select row_number() over (order by dummy) from (select * from remote('127.0.0.{1,2}', system, one));
