set dialect='kusto';
print ' -- binary functions';
print binary_and(4,7), binary_or(4,7);
print binary_shift_left(1, 1) == binary_shift_left(1, 65);
print binary_shift_right(2, 1) == binary_shift_right(2, 65);
print binary_shift_right(binary_shift_left(1, 65), 65) == 1;
print binary_xor(2, 5), bitset_count_ones(42);
print bitset_count_ones(binary_shift_left(binary_and(4,7), 1));
