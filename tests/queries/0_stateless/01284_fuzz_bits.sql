SELECT fuzzBits(toString('string'), 1); -- { serverError 43 }
SELECT fuzzBits('', 0.3); -- { serverError 44 }
SELECT length(fuzzBits(randomString(100), 0.5));
SELECT toTypeName(fuzzBits(randomString(100), 0.5));
