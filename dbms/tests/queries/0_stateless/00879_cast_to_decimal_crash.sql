select cast(toIntervalDay(1) as Nullable(Decimal(10, 10))); -- { serverError 70 }
