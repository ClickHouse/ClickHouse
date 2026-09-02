SELECT if(materialize(1) > 0, CAST(NULL, 'Nullable(Int64)'), materialize(toInt32(1)));
SELECT if(materialize(1) > 0, materialize(toInt32(1)), CAST(NULL, 'Nullable(Int64)'));
SELECT if(materialize(1) > 0, CAST(NULL, 'Nullable(Decimal(18, 4))'), materialize(CAST(2, 'Nullable(Decimal(9, 4))')));
SELECT if(materialize(1) > 0, materialize(CAST(2, 'Nullable(Decimal(9, 4))')), CAST(NULL, 'Nullable(Decimal(18, 4))'));
