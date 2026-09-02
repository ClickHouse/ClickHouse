-- { echoOn }
select startsWithUTF8('富强民主文明和谐', '富强');
select startsWithUTF8('富强民主文明和谐', '\xe5');
select startsWithUTF8('富强民主文明和谐', '');

SELECT startsWithUTF8('123', '123');
SELECT startsWithUTF8('123', '12');
SELECT startsWithUTF8('123', '1234');
SELECT startsWithUTF8('123', '');

select endsWithUTF8('富强民主文明和谐', '和谐');
select endsWithUTF8('富强民主文明和谐', '\x90');
select endsWithUTF8('富强民主文明和谐', '');

SELECT endsWithUTF8('123', '3');
SELECT endsWithUTF8('123', '23');
SELECT endsWithUTF8('123', '32');
SELECT endsWithUTF8('123', '');
-- { echoOff }
