-- { echo }
-- The printed fraction is rounded up to the next multiple of three that keeps every significant
-- digit. Before the fix the round-up was capped at 6, so a scale 7/8/9 value lost every digit
-- past the sixth.

SET date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1;

-- A. DateTime64: digits past position 5 must survive (the reported truncation).
SELECT 'A dt64 s9 .123456789', toString(toDateTime64('2024-01-01 00:00:00.123456789', 9, 'UTC'));
SELECT 'A dt64 s9 .000000700', toString(toDateTime64('2024-01-01 00:00:00.000000700', 9, 'UTC'));
SELECT 'A dt64 s9 .000000070', toString(toDateTime64('2024-01-01 00:00:00.000000070', 9, 'UTC'));
SELECT 'A dt64 s9 .000000007', toString(toDateTime64('2024-01-01 00:00:00.000000007', 9, 'UTC'));
SELECT 'A dt64 s8 .12345678', toString(toDateTime64('2024-01-01 00:00:00.12345678', 8, 'UTC'));
SELECT 'A dt64 s8 .00000078', toString(toDateTime64('2024-01-01 00:00:00.00000078', 8, 'UTC'));
SELECT 'A dt64 s8 .00000007', toString(toDateTime64('2024-01-01 00:00:00.00000007', 8, 'UTC'));
-- Scale 7/8 pad up to width 9, matching how the setting already pads at low scales (see group B).
SELECT 'A dt64 s7 .1234567', toString(toDateTime64('2024-01-01 00:00:00.1234567', 7, 'UTC'));
SELECT 'A dt64 s7 .0000007', toString(toDateTime64('2024-01-01 00:00:00.0000007', 7, 'UTC'));
-- The stored value really is non-zero, so the six zeros master printed were wrong, not coarse.
SELECT 'A raw ticks', toUnixTimestamp64Nano(toDateTime64('2024-01-01 00:00:00.000000700', 9, 'UTC')) % 1000000000;

-- B. Must not change: the (0, 3, 6) contract and the pre-existing padding at scales below 7.
-- The scale 1/2/4/5 rows are why clamping the round-up to the declared scale was rejected:
-- clamping would print widths that are not multiples of three here.
SELECT 'B dt64 s6 .123456', toString(toDateTime64('2024-01-01 00:00:00.123456', 6, 'UTC'));
SELECT 'B dt64 s6 .123400', toString(toDateTime64('2024-01-01 00:00:00.123400', 6, 'UTC'));
SELECT 'B dt64 s6 .120000', toString(toDateTime64('2024-01-01 00:00:00.120000', 6, 'UTC'));
SELECT 'B dt64 s6 .000000', toString(toDateTime64('2024-01-01 00:00:00.000000', 6, 'UTC'));
SELECT 'B dt64 s5 .12345', toString(toDateTime64('2024-01-01 00:00:00.12345', 5, 'UTC'));
SELECT 'B dt64 s5 .00007', toString(toDateTime64('2024-01-01 00:00:00.00007', 5, 'UTC'));
SELECT 'B dt64 s5 .00070', toString(toDateTime64('2024-01-01 00:00:00.00070', 5, 'UTC'));
SELECT 'B dt64 s4 .1234', toString(toDateTime64('2024-01-01 00:00:00.1234', 4, 'UTC'));
SELECT 'B dt64 s4 .0007', toString(toDateTime64('2024-01-01 00:00:00.0007', 4, 'UTC'));
SELECT 'B dt64 s3 .123', toString(toDateTime64('2024-01-01 00:00:00.123', 3, 'UTC'));
SELECT 'B dt64 s2 .05', toString(toDateTime64('2024-01-01 00:00:00.05', 2, 'UTC'));
SELECT 'B dt64 s2 .07', toString(toDateTime64('2024-01-01 00:00:00.07', 2, 'UTC'));
SELECT 'B dt64 s1 .5', toString(toDateTime64('2024-01-01 00:00:00.5', 1, 'UTC'));
SELECT 'B dt64 s1 .7', toString(toDateTime64('2024-01-01 00:00:00.7', 1, 'UTC'));
SELECT 'B dt64 s0', toString(toDateTime64('2024-01-01 00:00:00', 0, 'UTC'));
SELECT 'B dt64 s9 .123456000', toString(toDateTime64('2024-01-01 00:00:00.123456000', 9, 'UTC'));
SELECT 'B dt64 s9 .123000000', toString(toDateTime64('2024-01-01 00:00:00.123000000', 9, 'UTC'));
SELECT 'B dt64 s9 .000000000', toString(toDateTime64('2024-01-01 00:00:00.000000000', 9, 'UTC'));
SELECT 'B dt64 s9 .100000000', toString(toDateTime64('2024-01-01 00:00:00.1', 9, 'UTC'));
SELECT 'B dt64 s9 .000100000', toString(toDateTime64('2024-01-01 00:00:00.0001', 9, 'UTC'));

-- C. Time64 carries the same defect and is fixed by the same rule. Asserted through
-- toString and CAST only: Time64 row output formats do not consult this setting yet.
SELECT 'C t64 s9 .123456789', toString(toTime64('100:00:00.123456789', 9));
SELECT 'C t64 s9 .000000700', toString(toTime64('100:00:00.000000700', 9));
SELECT 'C t64 s9 .000000007', toString(toTime64('100:00:00.000000007', 9));
SELECT 'C t64 s8 .00000078', toString(toTime64('100:00:00.00000078', 8));
SELECT 'C t64 s7 .1234567', toString(toTime64('100:00:00.1234567', 7));
SELECT 'C t64 s9 negative', toString(toTime64('-100:00:00.123456789', 9));
SELECT 'C t64 s9 cast', CAST(toTime64('100:00:00.123456789', 9) AS String);
-- A scale 7 value discriminates three ways: .123456 before the fix, .123456700 after it, and
-- .1234567 if the route stopped consulting the setting at all.
SELECT 'C t64 s7 cast', CAST(toTime64('100:00:00.1234567', 7) AS String);
SELECT 'C t64 s6 .123456', toString(toTime64('100:00:00.123456', 6));
SELECT 'C t64 s4 .0007', toString(toTime64('100:00:00.0007', 4));
SELECT 'C t64 s1 .7', toString(toTime64('100:00:00.7', 1));
-- An all-zero fraction keeps three digits for `Time64`, because its writer always emits the
-- delimiter, whereas the `DateTime64` writer drops the fraction. Pre-existing and unchanged by the
-- new rule (both the old and new formulas yield width three here); pinned so the two stay described.
SELECT 'C t64 s9 zero', toString(toTime64('100:00:00.000000000', 9));
SELECT 'C t64 s1 zero', toString(toTime64('100:00:00.0', 1));
SELECT 'C dt64 s9 zero', toString(toDateTime64('2024-01-01 00:00:00.000000000', 9, 'UTC'));
-- Nested `Time64` goes through `SerializationTime64::serializeText`, which ignores this setting, so
-- the declared scale is printed. Scale 7 discriminates: .1234567 untrimmed, .123456700 trimmed. The
-- `DateTime64` twins trim, so the gap is `Time64`-specific.
SELECT 'C t64 s7 tuple untrimmed', toString(tuple(toTime64('100:00:00.1234567', 7)));
SELECT 'C t64 s7 array untrimmed', toString([toTime64('100:00:00.1234567', 7)]);
SELECT 'C t64 s7 map untrimmed', toString(map('k', toTime64('100:00:00.1234567', 7)));
SELECT 'C dt64 s7 tuple trimmed', toString(tuple(toDateTime64('2024-01-01 00:00:00.1234567', 7, 'UTC')));
SELECT 'C dt64 s7 array trimmed', toString([toDateTime64('2024-01-01 00:00:00.1234567', 7, 'UTC')]);

-- D. More than one route reaches the fixed code: the conversion functions and the serialization.
SELECT 'D cast', CAST(toDateTime64('2024-01-01 00:00:00.123456789', 9, 'UTC') AS String);
SELECT 'D tuple', toString(tuple(toDateTime64('2024-01-01 00:00:00.123456789', 9, 'UTC')));
SELECT 'D array', toString([toDateTime64('2024-01-01 00:00:00.123456789', 9, 'UTC')]);
SELECT 'D nullable', toString(toNullable(toDateTime64('2024-01-01 00:00:00.123456789', 9, 'UTC')));
SELECT toDateTime64('2024-01-01 00:00:00.123456789', 9, 'UTC') AS d FORMAT TSV;
-- The scale 9 rows above pin the reported output, but a value whose every digit is significant
-- prints nine digits with the setting on or off, so each route also gets a scale 7 twin: those
-- read .123456700 only when the route consults the setting AND the round-up is uncapped.
SELECT 'D cast s7', CAST(toDateTime64('2024-01-01 00:00:00.1234567', 7, 'UTC') AS String);
SELECT 'D tuple s7', toString(tuple(toDateTime64('2024-01-01 00:00:00.1234567', 7, 'UTC')));
SELECT 'D array s7', toString([toDateTime64('2024-01-01 00:00:00.1234567', 7, 'UTC')]);
SELECT 'D nullable s7', toString(toNullable(toDateTime64('2024-01-01 00:00:00.1234567', 7, 'UTC')));
SELECT toDateTime64('2024-01-01 00:00:00.1234567', 7, 'UTC') AS d FORMAT TSV;
SELECT toDateTime64('2024-01-01 00:00:00.1234567', 7, 'UTC') AS d FORMAT JSONEachRow;
-- Row output honours the setting only under the default 'simple' mode: 'iso' and 'unix_timestamp'
-- always print the declared scale.
SELECT toDateTime64('2024-01-01 00:00:00.123000000', 9, 'UTC') AS d SETTINGS date_time_output_format = 'iso' FORMAT TSV;
SELECT toDateTime64('2024-01-01 00:00:00.123000000', 9, 'UTC') AS d SETTINGS date_time_output_format = 'unix_timestamp' FORMAT TSV;
SELECT 'D toString under iso', toString(toDateTime64('2024-01-01 00:00:00.123000000', 9, 'UTC')) SETTINGS date_time_output_format = 'iso';
SELECT toDateTime64('2024-01-01 00:00:00.000000700', 9, 'UTC') AS d FORMAT TSV;
SELECT toDateTime64('2024-01-01 00:00:00.123456789', 9, 'UTC') AS d FORMAT JSONEachRow;

-- E. With the setting off the full declared scale is printed, unchanged.
SELECT 'E off s9', toString(toDateTime64('2024-01-01 00:00:00.123456789', 9, 'UTC')) SETTINGS date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 0;
SELECT 'E off s7', toString(toDateTime64('2024-01-01 00:00:00.0000007', 7, 'UTC')) SETTINGS date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 0;
SELECT 'E off s1', toString(toDateTime64('2024-01-01 00:00:00.5', 1, 'UTC')) SETTINGS date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 0;
SELECT 'E off t64 s9', toString(toTime64('100:00:00.123456789', 9)) SETTINGS date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 0;
