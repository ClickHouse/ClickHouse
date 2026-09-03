SELECT toTypeName([toLowCardinality('1'), toLowCardinality('2')]);
SELECT toTypeName([materialize(toLowCardinality('1')), toLowCardinality('2')]);
SELECT toTypeName([toLowCardinality('1'), materialize(toLowCardinality('2'))]);
SELECT toTypeName([materialize(toLowCardinality('1')), materialize(toLowCardinality('2'))]);

SELECT toTypeName([toLowCardinality('1'), '2']);
SELECT toTypeName([materialize(toLowCardinality('1')), '2']);
SELECT toTypeName([toLowCardinality('1'), materialize('2')]);
SELECT toTypeName([materialize(toLowCardinality('1')), materialize('2')]);

SELECT toTypeName(map(toLowCardinality('1'), toLowCardinality('2')));
SELECT toTypeName(map(materialize(toLowCardinality('1')), toLowCardinality('2')));
SELECT toTypeName(map(toLowCardinality('1'), materialize(toLowCardinality('2'))));
SELECT toTypeName(map(materialize(toLowCardinality('1')), materialize(toLowCardinality('2'))));
