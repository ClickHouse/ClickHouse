SET enable_analyzer = 0;
set join_algorithm = 'hash';

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select toLowCardinality(number) k, toString(number) s from numbers(2)) as js1
join (select number+1 k, toLowCardinality(toString(number+1)) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select number k, toLowCardinality(toString(number)) s from numbers(2)) as js1
join (select toLowCardinality(number+1) k, toString(number+1) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select toLowCardinality(number) k, toLowCardinality(toString(number)) s from numbers(2)) as js1
join (select toLowCardinality(number+1) k, toLowCardinality(toString(number+1)) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select toLowCardinality(number) k, toString(number) s from numbers(2)) as js1
full join (select number+1 k, toLowCardinality(toString(number+1)) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select number k, toLowCardinality(toString(number)) s from numbers(2)) as js1
full join (select toLowCardinality(number+1) k, toString(number+1) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select toLowCardinality(number) k, toLowCardinality(toString(number)) s from numbers(2)) as js1
full join (select toLowCardinality(number+1) k, toLowCardinality(toString(number+1)) s from numbers(2)) as js2
using k order by js1.k, js2.k;

set join_algorithm = 'partial_merge';

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select toLowCardinality(number) k, toString(number) s from numbers(2)) as js1
join (select number+1 k, toLowCardinality(toString(number+1)) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select number k, toLowCardinality(toString(number)) s from numbers(2)) as js1
join (select toLowCardinality(number+1) k, toString(number+1) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select toLowCardinality(number) k, toLowCardinality(toString(number)) s from numbers(2)) as js1
join (select toLowCardinality(number+1) k, toLowCardinality(toString(number+1)) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select toLowCardinality(number) k, toString(number) s from numbers(2)) as js1
full join (select number+1 k, toLowCardinality(toString(number+1)) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select number k, toLowCardinality(toString(number)) s from numbers(2)) as js1
full join (select toLowCardinality(number+1) k, toString(number+1) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select toLowCardinality(number) k, toLowCardinality(toString(number)) s from numbers(2)) as js1
full join (select toLowCardinality(number+1) k, toLowCardinality(toString(number+1)) s from numbers(2)) as js2
using k order by js1.k, js2.k;

SET enable_analyzer = 1;
set join_algorithm = 'hash';

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select toLowCardinality(number) k, toString(number) s from numbers(2)) as js1
join (select number+1 k, toLowCardinality(toString(number+1)) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select number k, toLowCardinality(toString(number)) s from numbers(2)) as js1
join (select toLowCardinality(number+1) k, toString(number+1) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select toLowCardinality(number) k, toLowCardinality(toString(number)) s from numbers(2)) as js1
join (select toLowCardinality(number+1) k, toLowCardinality(toString(number+1)) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select toLowCardinality(number) k, toString(number) s from numbers(2)) as js1
full join (select number+1 k, toLowCardinality(toString(number+1)) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select number k, toLowCardinality(toString(number)) s from numbers(2)) as js1
full join (select toLowCardinality(number+1) k, toString(number+1) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select toLowCardinality(number) k, toLowCardinality(toString(number)) s from numbers(2)) as js1
full join (select toLowCardinality(number+1) k, toLowCardinality(toString(number+1)) s from numbers(2)) as js2
using k order by js1.k, js2.k;
