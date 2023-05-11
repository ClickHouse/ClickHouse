select generateRandomStructure(5, 42);
select toTypeName(generateRandomStructure(5, 42));
select toColumnTypeName(generateRandomStructure(5, 42));
SELECT * FROM generateRandom(generateRandomStructure(5, 42), 42) LIMIT 1;

select generateRandomStructure(5, 42, 42); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select generateRandomStructure('5'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select generateRandomStructure(5, '42'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select generateRandomStructure(materialize(5), 42); -- {serverError ILLEGAL_COLUMN}
select generateRandomStructure(5, materialize(42)); -- {serverError ILLEGAL_COLUMN}

