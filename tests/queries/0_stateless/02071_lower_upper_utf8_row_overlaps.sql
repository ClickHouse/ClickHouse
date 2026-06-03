-- Tags: no-fasttest
-- no-fasttest: upper/lowerUTF8 use ICU

drop table if exists utf8_overlap;
create table utf8_overlap (str String) engine=Memory();

-- { echoOn }
-- NOTE: total string size should be > 16 (sizeof(__m128i))
insert into utf8_overlap values ('\xe2'), ('Foo⚊BarBazBam'), ('\xe2'), ('Foo⚊BarBazBam');
--                                             ^
--                                             MONOGRAM FOR YANG
with lowerUTF8(str) as l_, upperUTF8(str) as u_, '0x' || hex(str) as h_
select length(str), if(l_ == '\xe2', h_, l_), if(u_ == '\xe2', h_, u_) from utf8_overlap format CSV;

-- NOTE: regression test for introduced bug
-- https://github.com/ClickHouse/ClickHouse/issues/42756
SELECT lowerUTF8('КВ АМ И СЖ');
SELECT upperUTF8('кв ам и сж');
SELECT lowerUTF8('КВ АМ И СЖ КВ АМ И СЖ');
SELECT upperUTF8('кв ам и сж кв ам и сж');
-- Test at 32 and 64 byte boundaries
SELECT lowerUTF8(repeat('0', 16) || 'КВ АМ И СЖ');
SELECT upperUTF8(repeat('0', 16) || 'кв ам и сж');
SELECT lowerUTF8(repeat('0', 48) || 'КВ АМ И СЖ');
SELECT upperUTF8(repeat('0', 48) || 'кв ам и сж');
