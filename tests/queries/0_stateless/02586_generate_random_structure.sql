select generateRandomStructure(5, 42);
select generateRandomStructure(5, 42, false);
select generateRandomStructure(5, 42, false, false);
select generateRandomStructure(5, 42, true, false);
select generateRandomStructure(5, 42, true, true, false);
select generateRandomStructure(5, 42, true, true, true, false);
select generateRandomStructure(5, 42, true, true, true, true, true);
select generateRandomStructure(5, 42, false, true, true);
select toTypeName(generateRandomStructure(5, 42));
select toColumnTypeName(generateRandomStructure(5, 42));
SELECT * FROM generateRandom(generateRandomStructure(3, 42), 42) LIMIT 1;

select generateRandomStructure(5, 42, false, false, false, false, true, 42); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select generateRandomStructure('5'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select generateRandomStructure(5, '42'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select generateRandomStructure(5, 42, 'false'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select generateRandomStructure(5, 42, false, 'false'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select generateRandomStructure(5, 42, false, false, 'false'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select generateRandomStructure(5, 42, false, false, false, 'false'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select generateRandomStructure(5, 42, false, false, false, false, 'false'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select generateRandomStructure(materialize(5), 42, false, false); -- {serverError ILLEGAL_COLUMN}
select generateRandomStructure(5, materialize(42), false, false); -- {serverError ILLEGAL_COLUMN}
select generateRandomStructure(5, 42, materialize(false), false); -- {serverError ILLEGAL_COLUMN}
select generateRandomStructure(5, 42, false, materialize(false)); -- {serverError ILLEGAL_COLUMN}
select generateRandomStructure(5, 42, false, false, materialize(false)); -- {serverError ILLEGAL_COLUMN}
select generateRandomStructure(5, 42, false, false, false, materialize(false)); -- {serverError ILLEGAL_COLUMN}
select generateRandomStructure(5, 42, false, false, false, materialize(false)); -- {serverError ILLEGAL_COLUMN}
select generateRandomStructure(5, 42, false, false, false, false, materialize(false)); -- {serverError ILLEGAL_COLUMN}

