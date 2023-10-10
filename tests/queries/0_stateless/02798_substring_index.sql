-- { echoOn }
select substringIndex('www.clickhouse.com', '.', -4);
select substringIndex('www.clickhouse.com', '.', -3);
select substringIndex('www.clickhouse.com', '.', -2);
select substringIndex('www.clickhouse.com', '.', -1);
select substringIndex('www.clickhouse.com', '.', 0);
select substringIndex('www.clickhouse.com', '.', 1);
select substringIndex('www.clickhouse.com', '.', 2);
select substringIndex('www.clickhouse.com', '.', 3);
select substringIndex('www.clickhouse.com', '.', 4);

select substringIndex(materialize('www.clickhouse.com'), '.', -4);
select substringIndex(materialize('www.clickhouse.com'), '.', -3);
select substringIndex(materialize('www.clickhouse.com'), '.', -2);
select substringIndex(materialize('www.clickhouse.com'), '.', -1);
select substringIndex(materialize('www.clickhouse.com'), '.', 0);
select substringIndex(materialize('www.clickhouse.com'), '.', 1);
select substringIndex(materialize('www.clickhouse.com'), '.', 2);
select substringIndex(materialize('www.clickhouse.com'), '.', 3);
select substringIndex(materialize('www.clickhouse.com'), '.', 4);

select substringIndex(materialize('www.clickhouse.com'), '.', materialize(-4));
select substringIndex(materialize('www.clickhouse.com'), '.', materialize(-3));
select substringIndex(materialize('www.clickhouse.com'), '.', materialize(-2));
select substringIndex(materialize('www.clickhouse.com'), '.', materialize(-1));
select substringIndex(materialize('www.clickhouse.com'), '.', materialize(0));
select substringIndex(materialize('www.clickhouse.com'), '.', materialize(1));
select substringIndex(materialize('www.clickhouse.com'), '.', materialize(2));
select substringIndex(materialize('www.clickhouse.com'), '.', materialize(3));
select substringIndex(materialize('www.clickhouse.com'), '.', materialize(4));

select substringIndex('www.clickhouse.com', '.', materialize(-4));
select substringIndex('www.clickhouse.com', '.', materialize(-3));
select substringIndex('www.clickhouse.com', '.', materialize(-2));
select substringIndex('www.clickhouse.com', '.', materialize(-1));
select substringIndex('www.clickhouse.com', '.', materialize(0));
select substringIndex('www.clickhouse.com', '.', materialize(1));
select substringIndex('www.clickhouse.com', '.', materialize(2));
select substringIndex('www.clickhouse.com', '.', materialize(3));
select substringIndex('www.clickhouse.com', '.', materialize(4));

select SUBSTRING_INDEX('www.clickhouse.com', '.', 2);

select substringIndex('www.clickhouse.com', '..', 2); -- { serverError BAD_ARGUMENTS }
select substringIndex('www.clickhouse.com', '', 2); -- { serverError BAD_ARGUMENTS }
select substringIndex('www.clickhouse.com', materialize('.'), 2); -- { serverError ILLEGAL_COLUMN }
select substringIndex('www.clickhouse.com', '.', cast(2 as Int128)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

select substringIndexUTF8('富强，民主，文明', '，', -4);
select substringIndexUTF8('富强，民主，文明', '，', -3);
select substringIndexUTF8('富强，民主，文明', '，', -2);
select substringIndexUTF8('富强，民主，文明', '，', -1);
select substringIndexUTF8('富强，民主，文明', '，', 0);
select substringIndexUTF8('富强，民主，文明', '，', 1);
select substringIndexUTF8('富强，民主，文明', '，', 2);
select substringIndexUTF8('富强，民主，文明', '，', 3);
select substringIndexUTF8('富强，民主，文明', '，', 4);

select substringIndexUTF8(materialize('富强，民主，文明'), '，', -4);
select substringIndexUTF8(materialize('富强，民主，文明'), '，', -3);
select substringIndexUTF8(materialize('富强，民主，文明'), '，', -2);
select substringIndexUTF8(materialize('富强，民主，文明'), '，', -1);
select substringIndexUTF8(materialize('富强，民主，文明'), '，', 0);
select substringIndexUTF8(materialize('富强，民主，文明'), '，', 1);
select substringIndexUTF8(materialize('富强，民主，文明'), '，', 2);
select substringIndexUTF8(materialize('富强，民主，文明'), '，', 3);
select substringIndexUTF8(materialize('富强，民主，文明'), '，', 4);

select substringIndexUTF8('富强，民主，文明', '，', materialize(-4));
select substringIndexUTF8('富强，民主，文明', '，', materialize(-3));
select substringIndexUTF8('富强，民主，文明', '，', materialize(-2));
select substringIndexUTF8('富强，民主，文明', '，', materialize(-1));
select substringIndexUTF8('富强，民主，文明', '，', materialize(0));
select substringIndexUTF8('富强，民主，文明', '，', materialize(1));
select substringIndexUTF8('富强，民主，文明', '，', materialize(2));
select substringIndexUTF8('富强，民主，文明', '，', materialize(3));
select substringIndexUTF8('富强，民主，文明', '，', materialize(4));

select substringIndexUTF8(materialize('富强，民主，文明'), '，', materialize(-4));
select substringIndexUTF8(materialize('富强，民主，文明'), '，', materialize(-3));
select substringIndexUTF8(materialize('富强，民主，文明'), '，', materialize(-2));
select substringIndexUTF8(materialize('富强，民主，文明'), '，', materialize(-1));
select substringIndexUTF8(materialize('富强，民主，文明'), '，', materialize(0));
select substringIndexUTF8(materialize('富强，民主，文明'), '，', materialize(1));
select substringIndexUTF8(materialize('富强，民主，文明'), '，', materialize(2));
select substringIndexUTF8(materialize('富强，民主，文明'), '，', materialize(3));
select substringIndexUTF8(materialize('富强，民主，文明'), '，', materialize(4));

select substringIndexUTF8('富强，民主，文明', '，，', 2); -- { serverError BAD_ARGUMENTS }
select substringIndexUTF8('富强，民主，文明', '', 2); -- { serverError BAD_ARGUMENTS }
select substringIndexUTF8('富强，民主，文明', materialize('，'), 2); -- { serverError ILLEGAL_COLUMN }
select substringIndexUTF8('富强，民主，文明', '，', cast(2 as Int128)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- { echoOff }
