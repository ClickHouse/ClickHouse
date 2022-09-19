drop table if exists utf8_overlap;
create table utf8_overlap (str String) engine=Memory();

-- { echoOn }
-- NOTE: total string size should be > 16 (sizeof(__m128i))
insert into utf8_overlap values ('\xe2'), ('Foo⚊BarBazBam'), ('\xe2'), ('Foo⚊BarBazBam');
--                                             ^
--                                             MONOGRAM FOR YANG
with lowerUTF8(str) as l_, upperUTF8(str) as u_, '0x' || hex(str) as h_
select length(str), if(l_ == '\xe2', h_, l_), if(u_ == '\xe2', h_, u_) from utf8_overlap format CSV;
