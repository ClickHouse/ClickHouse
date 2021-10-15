-- { echo }
explain syntax select null is null;
explain syntax select null is not null;
explain syntax select isNull(null);
explain syntax select isNotNull(null);
