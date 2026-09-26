-- Regression test: arrayWithConstant with materialized FixedString used to throw
-- LOGICAL_ERROR due to byteSize() overhead mismatch in ColumnFixedString.
SELECT arrayWithConstant(2, materialize(toFixedString('qwerty', 6)));
