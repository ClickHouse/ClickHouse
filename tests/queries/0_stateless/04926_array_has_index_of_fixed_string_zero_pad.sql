-- https://github.com/ClickHouse/ClickHouse/issues/115311
-- has()/indexOf() on Array(FixedString) must zero-pad FixedString values of different
-- declared widths the same way equals() does, and must agree regardless of whether the
-- array argument is constant or materialized.

-- reference: equals() already implements the correct zero-pad semantics
select equals(toFixedString('V0', 3), toFixedString('V0', 4));

-- constant array, constant needle -- this was the broken case (used to return 0)
select has([toFixedString('V0', 3)], toFixedString('V0', 4));
select has([toFixedString('V0', 4)], toFixedString('V0', 3));
select indexOf([toFixedString('V0', 3)], toFixedString('V0', 4));
select indexOf([toFixedString('V0', 4)], toFixedString('V0', 3));

-- materialized array, constant needle -- was already correct, must not regress
select has(materialize([toFixedString('V0', 3)]), toFixedString('V0', 4));
select indexOf(materialize([toFixedString('V0', 3)]), toFixedString('V0', 4));

-- constant array, materialized needle
select has([toFixedString('V0', 3)], materialize(toFixedString('V0', 4)));
select indexOf([toFixedString('V0', 3)], materialize(toFixedString('V0', 4)));

-- both materialized
select has(materialize([toFixedString('V0', 3)]), materialize(toFixedString('V0', 4)));
select indexOf(materialize([toFixedString('V0', 3)]), materialize(toFixedString('V0', 4)));

-- FixedString array element vs plain String needle -- also zero-pads (issue notes this
-- direction separately, since it flips which side has to be padded)
select has([toFixedString('V0', 3)], 'V0');
select has(materialize([toFixedString('V0', 3)]), 'V0');

-- negative control: same width, different content -- must stay 0, not accidentally pass
select has([toFixedString('AB', 2)], toFixedString('AC', 2));
select has([toFixedString('AB', 2)], toFixedString('AB', 2));

-- negative control: plain String vs String must NEVER be zero-padded (different from
-- FixedString padding -- this is normal, length-sensitive string equality)
select has(['V0'], 'V0\0');
select has(['ab'], 'abc');

-- multi-element array, position check
select indexOf([toFixedString('x', 1), toFixedString('V0', 3)], toFixedString('V0', 4));

-- cross-check against arrayExists as an independent ground truth
select has([toFixedString('V0', 3)], toFixedString('V0', 4)) = arrayExists(x -> x = toFixedString('V0', 4), [toFixedString('V0', 3)]);
