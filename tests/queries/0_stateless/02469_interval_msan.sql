SELECT now() + 1::Int128; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT now() + 1::Int256; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT now() + 1::UInt128; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT now() + 1::UInt256; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT now() - 1::Int128; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT now() - 1::Int256; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT now() - 1::UInt128; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT now() - 1::UInt256; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT now() + INTERVAL 1::Int128 SECOND - now();
SELECT now() + INTERVAL 1::Int256 SECOND - now();
SELECT now() + INTERVAL 1::UInt128 SECOND - now();
SELECT now() + INTERVAL 1::UInt256 SECOND - now();

SELECT today() + INTERVAL 1::Int128 DAY - today();
SELECT today() + INTERVAL 1::Int256 DAY - today();
SELECT today() + INTERVAL 1::UInt128 DAY - today();
SELECT today() + INTERVAL 1::UInt256 DAY - today();
