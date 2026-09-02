set short_circuit_function_evaluation='force_enable';

select 'if with one LC argument';
select if(0, toLowCardinality('a'), 'b');
select if(1, toLowCardinality('a'), 'b');
select if(materialize(0), materialize(toLowCardinality('a')), materialize('b'));
select if(number % 2, toLowCardinality('a'), 'b') from numbers(2);
select if(number % 2, materialize(toLowCardinality('a')), materialize('b')) from numbers(2);

select 'if with LC and NULL arguments';
select if(0, toLowCardinality('a'), NULL);
select if(1, toLowCardinality('a'), NULL);
select if(materialize(0), materialize(toLowCardinality('a')), NULL);
select if(number % 2, toLowCardinality('a'), NULL) from numbers(2);
select if(number % 2, materialize(toLowCardinality('a')), NULL) from numbers(2);

select 'if with two LC arguments';
select if(0, toLowCardinality('a'), toLowCardinality('b'));
select if(1, toLowCardinality('a'), toLowCardinality('b'));
select if(materialize(0), materialize(toLowCardinality('a')), materialize(toLowCardinality('b')));
select if(number % 2, toLowCardinality('a'), toLowCardinality('b')) from numbers(2);
select if(number % 2, materialize(toLowCardinality('a')), materialize(toLowCardinality('a'))) from numbers(2);

select if(number % 2, toLowCardinality(number), NULL) from numbers(2);
select if(number % 2, toLowCardinality(number), toLowCardinality(number + 1)) from numbers(2);

