set enable_dynamic_type=1;
select not materialize(1)::Dynamic as res, toTypeName(res);
select xor(materialize(1)::Dynamic, materialize(0)::Dynamic) as res, toTypeName(res);

SELECT sum((NOT CAST(materialize(1) AS Dynamic)) = TRUE);

