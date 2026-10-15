set session_timezone='UTC';

select JSONExtract('{"utc" : 1747771112221}', 'utc', 'DateTime64(3)');
select JSONExtract('{"utc" : -1747771112221}', 'utc', 'DateTime64(3)');
select '{"utc" : 1747771112221}'::JSON(utc DateTime64);
select '{"utc" : -1747771112221}'::JSON(utc DateTime64);


