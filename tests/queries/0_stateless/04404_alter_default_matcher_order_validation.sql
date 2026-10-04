DROP TABLE IF EXISTS alter_default_matcher_add_first_validation;
DROP TABLE IF EXISTS alter_default_matcher_modify_after_validation;

CREATE TABLE alter_default_matcher_add_first_validation
(
    a String,
    x UInt8,
    b UInt64 DEFAULT length(tupleElement(tuple(* EXCEPT b), 1))
)
ENGINE = Memory;

ALTER TABLE alter_default_matcher_add_first_validation ADD COLUMN y UInt8 FIRST; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

CREATE TABLE alter_default_matcher_modify_after_validation
(
    a UInt8,
    s String,
    x UInt8,
    b UInt64 DEFAULT length(tupleElement(tuple(* EXCEPT b), 2))
)
ENGINE = Memory;

ALTER TABLE alter_default_matcher_modify_after_validation MODIFY COLUMN x UInt8 AFTER a; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE alter_default_matcher_modify_after_validation;
DROP TABLE alter_default_matcher_add_first_validation;
