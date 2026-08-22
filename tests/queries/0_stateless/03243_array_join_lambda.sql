SELECT arrayMap(x -> (x + length(arrayJoin([arrayMap(y -> (y + 1), [3])]))), [1, 2]);
