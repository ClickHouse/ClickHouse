select generateRandomStructure(5, 42);
select generateRandomStructure(5, 42, false);
select generateRandomStructure(5, 42, false, false);
select generateRandomStructure(5, 42, true, false);
select toTypeName(generateRandomStructure(5, 42));
select toColumnTypeName(generateRandomStructure(5, 42));
SELECT * FROM generateRandom(generateRandomStructure(3, 24), 24) LIMIT 1;

select generateRandomStructure(5, 42, false, false, 42); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select generateRandomStructure('5'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select generateRandomStructure(5, '42'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select generateRandomStructure(5, 42, 'false'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select generateRandomStructure(5, 42, false, 'false'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select generateRandomStructure(materialize(5), 42, false, false); -- {serverError ILLEGAL_COLUMN}
select generateRandomStructure(5, materialize(42), false, false); -- {serverError ILLEGAL_COLUMN}
select generateRandomStructure(5, 42, materialize(false), false); -- {serverError ILLEGAL_COLUMN}
select generateRandomStructure(5, 42, false, materialize(false)); -- {serverError ILLEGAL_COLUMN}
