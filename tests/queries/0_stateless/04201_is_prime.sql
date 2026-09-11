-- { echo }

-- Tiny values: smallest primes and composites
SELECT isPrime(0), isPrime(1), isPrime(2), isPrime(3), isPrime(4), isPrime(5);
SELECT isProbablePrime(0), isProbablePrime(1), isProbablePrime(2), isProbablePrime(3);

-- Type dispatch: largest prime per supported type
SELECT
    isPrime(toUInt8(251)),
    isPrime(toUInt16(65521)),
    isPrime(toUInt32(4294967291)),
    isPrime(toUInt64(18446744073709551557));
SELECT
    isProbablePrime(toUInt128('170141183460469231731687303715884105727')),
    isProbablePrime(toUInt256('57896044618658097711785492504343953926634992332820282019728792003956564819949'));

-- Carmichael numbers (must all return 0; they fool any Fermat-only test)
SELECT isPrime(561), isPrime(1105), isPrime(1729), isPrime(2465),
       isPrime(2821), isPrime(6601), isPrime(8911), isPrime(10585),
       isPrime(15841), isPrime(29341), isPrime(41041), isPrime(46657),
       isPrime(52633), isPrime(62745), isPrime(63973), isPrime(75361);

-- Strong pseudoprimes to small base sets (smallest in each class)
SELECT isPrime(2047);
SELECT isPrime(3215031751);
SELECT isPrime(1373653);
SELECT isPrime(25326001);
SELECT isPrime(4759123141);
SELECT isPrime(toUInt64(3825123056546413051));

-- Largest prime below the type ceiling, and the type maximum itself
SELECT isPrime(toUInt8(251));
SELECT isPrime(toUInt16(65521));
SELECT isPrime(toUInt32(4294967291));
SELECT isPrime(toUInt64(18446744073709551557));
SELECT isProbablePrime(toUInt128('340282366920938463463374607431768211297'));
SELECT isProbablePrime(toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639747'));
SELECT isPrime(toUInt8(255));
SELECT isPrime(toUInt32(4294967295));
SELECT isPrime(toUInt64(18446744073709551615));

-- Adversarial bit patterns near 2^k boundaries
WITH arrayJoin([
    (8,  toUInt64(253)),  (8,  toUInt64(255)),  (8,  toUInt64(257)),  (8,  toUInt64(259)),
    (16, toUInt64(65533)),(16, toUInt64(65535)),(16, toUInt64(65537)),(16, toUInt64(65539)),
    (32, toUInt64(4294967293)),         (32, toUInt64(4294967295)),
    (32, toUInt64(4294967297)),         (32, toUInt64(4294967299)),
    (63, toUInt64(9223372036854775805)),(63, toUInt64(9223372036854775807)),
    (63, toUInt64(9223372036854775809)),(63, toUInt64(9223372036854775811)),
    (64, toUInt64(18446744073709551613)),(64, toUInt64(18446744073709551615))
]) AS row
SELECT row.1 AS k, row.2 AS n, isProbablePrime(n);

-- Mersenne primes M_31, M_61, M_127
SELECT isPrime(toUInt32(2147483647));
SELECT isPrime(toUInt64(2305843009213693951));
SELECT isProbablePrime(toUInt128('170141183460469231731687303715884105727'));

-- Fermat numbers F_0..F_4 (all prime), F_5 (composite)
SELECT isPrime(3), isPrime(5), isPrime(17), isPrime(257), isPrime(65537);
SELECT isPrime(toUInt64(4294967297));

-- Squares of primes near type boundaries
SELECT isPrime(toUInt32(65521 * 65521));
SELECT isPrime(toUInt64(4294967291) * toUInt64(4294967291));
SELECT isProbablePrime(toUInt128('170141183460469231731687303715884105727') * toUInt128(2));

-- NULL propagates
SELECT isPrime(CAST(NULL AS Nullable(UInt32)));
SELECT isProbablePrime(CAST(NULL AS Nullable(UInt256)));
SELECT isProbablePrime(CAST(NULL AS Nullable(UInt64)), 5);
WITH arrayJoin([NULL, 0, 1, 2, 3, 561, 17]) AS x
SELECT x, isPrime(CAST(x AS Nullable(UInt16))) FROM numbers(1) FORMAT TSV;

-- LowCardinality(UInt*)
SELECT isPrime(CAST(17 AS LowCardinality(UInt32))), isPrime(CAST(18 AS LowCardinality(UInt32)))
SETTINGS allow_suspicious_low_cardinality_types = 1;

-- Argument validation
SELECT isProbablePrime(toUInt64(17), number) FROM numbers(1); -- { serverError ILLEGAL_COLUMN }
WITH arrayJoin(
    [561, 1105, 1729, 2047, 2465, 2821, 6601, 8911, 1373653, 25326001,
     3215031751, 4759123141, 18446744073709551615]
) AS c, arrayJoin(range(1, 33)) AS r
SELECT sum(isProbablePrime(toUInt64(c), r));                 -- { serverError ILLEGAL_COLUMN }
SELECT isProbablePrime(toUInt64(17), 0);                     -- { serverError BAD_ARGUMENTS }
SELECT isProbablePrime(toUInt64(17), -1);                    -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isProbablePrime(toUInt64(17), 5.5);                   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isPrime(toUInt128(17));                               -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isPrime(toUInt256(17));                               -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isPrime(toInt32(17));                                 -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isPrime(toFloat64(17));                               -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isPrime('17');                                        -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isPrime();                                            -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT isPrime(17, 5);                                       -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT isProbablePrime();                                    -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT isProbablePrime(17, 5, 3);                            -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
