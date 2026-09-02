select d from values('d Decimal(8, 8)', 0, 1) where d not in (-1, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
select d from values('d Decimal(8, 8)', 0, 2) where d not in (1, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
select d from values('d Decimal(9, 8)', 0, 3) where d not in (-9223372036854775808, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
select d from values('d Decimal(9, 8)', 0, 4) where d not in (18446744073709551615, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
select d from values('d Decimal(18, 8)', 0, 5) where d not in (-9223372036854775808, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
select d from values('d Decimal(18, 8)', 0, 6) where d not in (18446744073709551615, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
select d from values('d Decimal(26, 8)', 0, 7) where d not in (-9223372036854775808, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
select d from values('d Decimal(27, 8)', 0, 8) where d not in (18446744073709551615, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
select d from values('d Decimal(27, 8)', 0, 9) where d not in (-9223372036854775808, 0);
select d from values('d Decimal(28, 8)', 0, 10) where d not in (18446744073709551615, 0);
