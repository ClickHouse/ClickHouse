-- Date and DateTime

SELECT reinterpretAsDate(); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT reinterpretAsDate('A',''); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT reinterpretAsDate([0, 1, 2]); -- { clientError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT reinterpretAsDateTime(); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT reinterpretAsDateTime('A',''); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT reinterpretAsDateTime([0, 1, 2]); -- { clientError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT reinterpretAsDate(65);
SELECT reinterpretAsDate('A');
SELECT reinterpretAsDateTime(65);
SELECT reinterpretAsDate('A');

-- Fixed String

SELECT reinterpretAsFixedString(); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT reinterpretAsFixedString(toDate('1970-01-01'),''); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT reinterpretAsFixedString([0, 1, 2]); -- { clientError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT reinterpretAsFixedString(toDate('1970-03-07'));
SELECT reinterpretAsFixedString(toDateTime('1970-01-01 01:01:05'));
SELECT reinterpretAsFixedString(65);

-- Float32, Float64

SELECT reinterpretAsFloat32(); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT reinterpretAsFloat64(); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT reinterpretAsFloat32('1970-01-01', ''); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT reinterpretAsFloat64('1970-01-01', ''); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT reinterpretAsFloat32([0, 1, 2]); -- { clientError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT reinterpretAsFloat64([0, 1, 2]); -- { clientError4 ILLEGAL_TYPE_OF_ARGUMENT}




