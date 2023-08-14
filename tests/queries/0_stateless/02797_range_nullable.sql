SELECT range(null);
SELECT range(10, null);
SELECT range(10, 2, null);
select range('string', Null);
SELECT range(toNullable(1));
SELECT range(0::Nullable(UInt64), 10::Nullable(UInt64), 2::Nullable(UInt64));
SELECT range(0::Nullable(Int64), 10::Nullable(Int64), 2::Nullable(Int64));
SELECT range(materialize(0), 10::Nullable(UInt64), 2::Nullable(UInt64));
SELECT range(Null::Nullable(UInt64), 10::Nullable(UInt64), 2::Nullable(UInt64)); -- { serverError BAD_ARGUMENTS }
SELECT range(0::Nullable(UInt64), Null::Nullable(UInt64), 2::Nullable(UInt64)); -- { serverError BAD_ARGUMENTS }
SELECT range(0::Nullable(UInt64), 10::Nullable(UInt64), Null::Nullable(UInt64)); -- { serverError BAD_ARGUMENTS }
SELECT range(Null::Nullable(UInt8), materialize(1)); -- { serverError BAD_ARGUMENTS }
