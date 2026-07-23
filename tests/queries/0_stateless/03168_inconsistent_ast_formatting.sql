create table a (x `Null`); -- { clientError SYNTAX_ERROR }
create table a (x f(`Null`)); -- { clientError SYNTAX_ERROR }
create table a (x Enum8(f(`Null`, 'World', 2))); -- { clientError SYNTAX_ERROR }
create table a (`value2` Enum8('Hello' = 1, equals(`Null`, 'World', 2), '!' = 3)); -- { clientError SYNTAX_ERROR }

create table a (x Int8) engine Memory;
create table b empty as a;

SELECT '--';
SELECT NOT (1);
SELECT formatQuery('SELECT NOT 1');
SELECT formatQuery('SELECT NOT (1)');

SELECT '--';
SELECT NOT (1, 1, 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT formatQuery('SELECT NOT (1, 1, 1)');
SELECT formatQuery('SELECT not(1, 1, 1)');

SELECT '--';
SELECT NOT ((1,)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT NOT tuple(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatQuery('SELECT NOT ((1,))');
SELECT formatQuery('SELECT NOT (tuple(1))');
SELECT formatQuery('SELECT NOT tuple(1)');

SELECT '--';
SELECT NOT ((1, 1, 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatQuery('SELECT NOT ((1, 1, 1))');
SELECT formatQuery('SELECT not((1, 1, 1))');
SELECT formatQuery('SELECT not tuple(1, 1, 1)');
SELECT formatQuery('SELECT not (tuple(1, 1, 1))');

SELECT '--';
SELECT NOT [1]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT NOT [(1)]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatQuery('SELECT NOT [1]');
SELECT formatQuery('SELECT NOT array(1)');
SELECT formatQuery('SELECT NOT (array(1))');
SELECT formatQuery('SELECT NOT [(1)]');
SELECT formatQuery('SELECT NOT ([1])');

SELECT '--';
SELECT -[1]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT -[(1)]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatQuery('SELECT -[1]');
SELECT formatQuery('SELECT -array(1)');
SELECT formatQuery('SELECT -(array(1))');
SELECT formatQuery('SELECT -[(1)]');
SELECT formatQuery('SELECT -([1])');

SELECT '--';
SELECT -(1, 1, 1);
SELECT formatQuery('SELECT -(1, 1, 1)');
SELECT formatQuery('SELECT negate ((1, 1, 1))');
SELECT formatQuery('SELECT -tuple(1, 1, 1)');
SELECT formatQuery('SELECT -(tuple(1, 1, 1))');

SELECT '--';
SELECT -tuple((1, 1, 1));
SELECT formatQuery('SELECT -((1, 1, 1))');
SELECT formatQuery('SELECT -tuple((1, 1, 1))');
