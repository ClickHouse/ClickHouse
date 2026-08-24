select toTypeName(materialize(js1.k)), toTypeName(materialize(js2.k)), toTypeName(materialize(js1.s)), toTypeName(materialize(js2.s))
from (select number k, toLowCardinality(toString(number)) s from numbers(2)) as js1
full join (select toLowCardinality(number+1) k, toString(number+1) s from numbers(2)) as js2
ON js1.k = js2.k order by js1.k, js2.k;

select toTypeName(js1.k), toTypeName(js2.k), toTypeName(js1.s), toTypeName(js2.s)
from (select number k, toLowCardinality(toString(number)) s from numbers(2)) as js1
full join (select toLowCardinality(number+1) k, toString(number+1) s from numbers(2)) as js2
ON js1.k = js2.k order by js1.k, js2.k;
