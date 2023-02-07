SELECT fromModifiedJulianDay(9223372036854775807 :: Int64); -- { serverError 490 }
