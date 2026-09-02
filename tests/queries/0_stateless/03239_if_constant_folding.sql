SET enable_analyzer = 1;

select false  ? c : '' as c, count() from (select '' c) group by c;
select if( 0 , c, '') _c, count() from (select '' c) group by _c;
select if(1 = 0, c, '') _c, count() from (select '' c) group by _c;
select materialize(false) ? c : 'x' as c, count() from (select 'o' c) group by c;
select if(1 = 0, c, '') _c, count() from (select '' c) group by _c;
select if(1 = 1, c, '') _c, count() from (select '' c) group by _c;

DROP TABLE IF EXISTS f;
CREATE TABLE f(c String) ENGINE = Null;

DROP TABLE IF EXISTS v;
create materialized view v engine = Null as
select
   false ? c : '' as c,
   countState() t
from f group by c;

DROP TABLE v;
DROP TABLE f;
