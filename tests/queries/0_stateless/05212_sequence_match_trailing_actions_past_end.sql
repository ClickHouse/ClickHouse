-- A pattern whose remaining actions are all satisfied by a zero-length match must stop at the
-- end of the action list. sequenceMatchEvents has no conditions_met early-out, so an event
-- list that stayed empty reaches the trailing actions with no events at all.
select [] = sequenceMatchEvents('')(t, c = 1, c = 2) from values('t UInt32, c UInt8', (0, 0), (1, 0));
select 1 = sequenceMatch('(?t<=10)')(t, c = 1, c = 2) from values('t UInt32, c UInt8', (0, 0), (1, 0));
-- The events can also run out part way through the pattern, leaving a trailing `.*`.
select 1 = sequenceCount('(?1).*')(t, c = 1, c = 2) from values('t UInt32, c UInt8', (0, 1));
-- A trailing action that a zero-length match does not satisfy still has to fail.
select 0 = sequenceMatch('(?1)(?t<=10)(?2)')(t, c = 1, c = 2) from values('t UInt32, c UInt8', (0, 1), (100, 2));

-- The four assertions above only catch the read with MemorySanitizer, which tracks that the byte at
-- `actions[size()]` was never written. The ones below make it leave the allocation as well, so
-- AddressSanitizer reports it too. `PatternActions` is a `PODArrayWithStackMemory<PatternAction, 64>`:
-- it keeps four actions inline and then grows to a heap block holding exactly a power of two of them,
-- so a pattern parsed into exactly eight actions ends with `size() == capacity()` and the action one
-- past the end is the first byte after the block. The parser always emits a leading `.*` of its own,
-- so seven actions of pattern text make eight.
select [1, 2, 3] = sequenceMatchEvents('(?1)(?2)(?3).*.*.*.*')(t, c = 1, c = 2, c = 3) from values('t UInt32, c UInt8', (1, 1), (2, 2), (3, 3));
select 1 = sequenceMatch('(?1)(?t<=100)(?2)(?3).*.*.*')(t, c = 1, c = 2, c = 3) from values('t UInt32, c UInt8', (1, 1), (2, 2), (3, 3));
select 1 = sequenceCount('(?1)(?t<=100)(?2)(?3).*.*.*')(t, c = 1, c = 2, c = 3) from values('t UInt32, c UInt8', (1, 1), (2, 2), (3, 3));
-- A temporal condition inside the trailing run is skipped just like a `.*`, and has to be skipped
-- through to the very end of the eight actions.
select [1, 2, 3] = sequenceMatchEvents('(?1)(?2)(?3).*(?t<=5).*.*')(t, c = 1, c = 2, c = 3) from values('t UInt32, c UInt8', (1, 1), (2, 2), (3, 3));
-- ... while a trailing `(?t>...)` stops the skip inside the action list, so the bound is not what ends it.
select 0 = sequenceMatch('(?1)(?t<=100)(?2)(?3).*.*(?t>1)')(t, c = 1, c = 2, c = 3) from values('t UInt32, c UInt8', (1, 1), (2, 2), (3, 3));
