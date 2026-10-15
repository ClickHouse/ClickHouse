set allow_experimental_kusto_dialect=1;
set dialect='kusto';
print ' -- binary functions';
-- [removed in the KQL rewrite] Received exception from server (version 26.8.1):
-- print binary_and(4,7), binary_or(4,7);
-- [removed in the KQL rewrite] Received exception from server (version 26.8.1):
-- print binary_xor(2, 5), bitset_count_ones(42);
-- [removed in the KQL rewrite] Received exception from server (version 26.8.1):
-- print bitset_count_ones(binary_shift_left(binary_and(4,7), 1));
