select number % 2 ? materialize(1)::Decimal(18, 10) : 2 FROM numbers(3);
