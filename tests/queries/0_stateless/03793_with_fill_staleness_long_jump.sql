-- Regression for the case when STALENESS jumps cannot handle big hops
SELECT arrayJoin([1844674407370955155::UInt64, 18446744073709551550::UInt64]) a ORDER BY a DESC WITH FILL STALENESS -3;
