SET enable_qbit_type = 1;

SELECT [0, 0, 0, 0, 0, 0]::QBit(BFloat16, 6) == [0, 0, 0, 0, 0, 0]::QBit(BFloat16, 6);
SELECT [0, 0, 0, 0, 0, 0]::QBit(BFloat16, 6) != [0, 0, 0, 0, 0, 0]::QBit(BFloat16, 6);
SELECT [0, 0, 0, 0, 0, 0]::QBit(Float32, 6) == [0, 0, 0, 0, 0, 0]::QBit(BFloat16, 6); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT [0, 0, 0, 0, 0, 0]::QBit(BFloat16, 6) == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]::QBit(BFloat16, 14); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
