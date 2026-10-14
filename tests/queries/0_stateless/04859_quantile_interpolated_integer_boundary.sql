-- The weighted interpolating quantiles must not narrow a `Float64`: `Int64::max` has no exact
-- `Float64` representation, and above magnitude 2^53 the `Float64` spacing exceeds one, so values
-- there are only intermittently representable.

-- Two ticks apart at level 0.5: the middle tick must be returned, not the lower endpoint.
SELECT toUnixTimestamp64Nano(quantileExactWeightedInterpolated(0.5)(x, 1)) FROM (SELECT arrayJoin([fromUnixTimestamp64Nano(1700000000000000000, 'UTC'), fromUnixTimestamp64Nano(1700000000000000002, 'UTC')]) AS x);

-- At the bounds, where the `Float64` lands on 2^63 exactly.
SELECT quantileInterpolatedWeighted(0.5)(x, 1) FROM (SELECT arrayJoin([toInt64(9223372036854775806), toInt64(9223372036854775807)]) AS x);
SELECT quantileInterpolatedWeighted(0.5)(x, 1) FROM (SELECT arrayJoin([toUInt64(18446744073709551613), toUInt64(18446744073709551615)]) AS x);
SELECT quantileInterpolatedWeighted(0.6)(x, 1) FROM (SELECT arrayJoin([toInt8(-128), toInt8(127)]) AS x);

-- Each level of a multi-level call is interpolated separately, so the levels must disagree.
SELECT arrayMap(v -> toUnixTimestamp64Nano(v), quantilesExactWeightedInterpolated(0.25, 0.75)(x, 1)) FROM (SELECT arrayJoin([fromUnixTimestamp64Nano(9223372036854775803, 'UTC'), fromUnixTimestamp64Nano(9223372036854775807, 'UTC')]) AS x);
SELECT quantilesInterpolatedWeighted(0.4, 0.6)(x, 1) FROM (SELECT arrayJoin([toInt64(9223372036854775803), toInt64(9223372036854775807)]) AS x);

-- The widest decimal types, with the endpoints a few ticks apart.
SELECT quantileInterpolatedWeighted(0.5)(x, 1) FROM (SELECT arrayJoin([toDecimal128('17014118346046923173168730371588410572', 0), toDecimal128('17014118346046923173168730371588410575', 0)]) AS x);
SELECT quantileInterpolatedWeighted(0.5)(x, 1) FROM (SELECT arrayJoin([toDecimal256('5789604461865809771178549250434395392663499233282028201972879200395656481996', 0), toDecimal256('5789604461865809771178549250434395392663499233282028201972879200395656481999', 0)]) AS x);

-- A span wider than 64 bits, so the intermediate really uses its upper limbs.
SELECT quantileExactWeightedInterpolated(0.7)(x, 1) FROM (SELECT arrayJoin([toDecimal128('-13407807929942597099574024998205846127', 0), toDecimal128('26815615859642947033519632392293427879', 0)]) AS x);
SELECT quantileExactWeightedInterpolated(0.7)(x, 1) FROM (SELECT arrayJoin([toDecimal256('-2894802230932904885589274625217197696331749616641014100986439600197828240998', 0), toDecimal256('5789604461865809771178549250434395392663499233282028201972879200395656481994', 0)]) AS x);
SELECT quantileInterpolatedWeighted(0.3)(x, 1) FROM (SELECT arrayJoin([toDecimal128('-13407807929942597099574024998205846127', 0), toDecimal128('26815615859642947033519632392293427879', 0)]) AS x);
SELECT quantileInterpolatedWeighted(0.7)(x, 1) FROM (SELECT arrayJoin([toDecimal256('-2894802230932904885589274625217197696331749616641014100986439600197828240998', 0), toDecimal256('5789604461865809771178549250434395392663499233282028201972879200395656481994', 0)]) AS x);

-- A span of exactly 2^64: the difference's low limb is zero, so the width comparison has to see
-- the upper one.
SELECT quantileInterpolatedWeighted(0.5)(x, 1) FROM (SELECT arrayJoin([toDecimal128('1700000000000000001', 0), toDecimal128('20146744073709551617', 0)]) AS x);

-- A level next to a percentile breakpoint, so the divisor spans more than one limb.
SELECT quantileInterpolatedWeighted(0.250000000931322574615478515625)(x, 1) FROM (SELECT arrayJoin([toDecimal128('-13407807929942597099574024998205846127', 0), toDecimal128('26815615859642947033519632392293427879', 0)]) AS x);

-- A level so small the weight has no representable fraction, so the offset is zero and only the
-- truncation fix-up moves the result.
SELECT quantileExactWeightedInterpolated(5e-324)(x, 1) FROM (SELECT arrayJoin([toDecimal128('-13407807929942597099574024998205846127', 0), toDecimal128('26815615859642947033519632392293427879', 0)]) AS x);
SELECT quantileInterpolatedWeighted(5e-324)(t.1, t.2) FROM (SELECT arrayJoin([(toDecimal128('-13407807929942597099574024998205846127', 0), toUInt64(0)), (toDecimal128('26815615859642947033519632392293427879', 0), toUInt64(1))]) AS t);

-- Truncation toward zero on the negative side moves the result up, not down.
SELECT quantilesInterpolatedWeighted(0.4, 0.6)(x, 1) FROM (SELECT arrayJoin([toInt64(-9223372036854775807), toInt64(-9223372036854775803)]) AS x);

-- Endpoints below the magnitude bound whose difference fits: unchanged from the last release.
SELECT quantileInterpolatedWeighted(0.6)(x, 1) FROM (SELECT arrayJoin([toUInt32(0), toUInt32(10)]) AS x);

-- A constant column above the magnitude bound, weighted: the ratio evaluator has its own
-- equal-endpoint return.
SELECT quantileInterpolatedWeighted(0.6)(x, 1) FROM (SELECT toDecimal128('1700000000000000001', 0) AS x FROM numbers(2));

-- The extreme levels over distinct endpoints wider than 64 bits: an endpoint is returned as
-- itself, not through the offset arithmetic.
SELECT quantileInterpolatedWeighted(0.25)(x, 1) FROM (SELECT arrayJoin([toDecimal128('-13407807929942597099574024998205846127', 0), toDecimal128('26815615859642947033519632392293427879', 0)]) AS x);
SELECT quantileInterpolatedWeighted(0.75)(x, 1) FROM (SELECT arrayJoin([toDecimal128('-13407807929942597099574024998205846127', 0), toDecimal128('26815615859642947033519632392293427879', 0)]) AS x);
SELECT quantilesInterpolatedWeighted(0.25, 0.75)(x, 1) FROM (SELECT arrayJoin([toDecimal128('-13407807929942597099574024998205846127', 0), toDecimal128('26815615859642947033519632392293427879', 0)]) AS x);

-- Coincident float endpoints are returned directly, so an infinite one does not form `inf * 0.0`.
SELECT quantileExactWeightedInterpolated(0.5)(x, 1) FROM (SELECT toFloat64('inf') AS x FROM numbers(2));
SELECT quantilesExactWeightedInterpolated(0, 0.5, 1)(x, 1) FROM (SELECT toFloat64('inf') AS x FROM numbers(2));
SELECT quantileExactWeightedInterpolated(0.5)(x, 1) FROM (SELECT toFloat64('-inf') AS x FROM numbers(2));
SELECT quantileExactWeightedInterpolated(0.5)(x, 1) FROM (SELECT toFloat64(7.5) AS x FROM numbers(2));

-- A level equal to a sample's own percentile returns that endpoint, below magnitude 2^53.
SELECT quantileInterpolatedWeighted(0.28429197623472124)(x, w) FROM (SELECT arrayJoin([(toInt64(0), toUInt64(253454710)), (toInt64(8816951084454569), toUInt64(325664384)), (toInt64(8816951084454570), toUInt64(110773682)), (toInt64(8816951084454571), toUInt64(774400756))]) AS t, t.1 AS x, t.2 AS w);
