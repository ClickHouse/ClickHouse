set enable_analyzer = 1;
-- { echo }
explain syntax select negate(1), negate(-1), - -1, -(-1), (-1) in (-1);
explain syntax select negate(1.), negate(-1.), - -1., -(-1.), (-1.) in (-1.);
explain syntax select negate(-9223372036854775808), -(-9223372036854775808), - -9223372036854775808;
explain syntax select negate(0), negate(-0), - -0, -(-0), (-0) in (-0);
explain syntax select negate(0.), negate(-0.), - -0., -(-0.), (-0.) in (-0.);
