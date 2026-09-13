-- A pattern whose remaining actions are all satisfied by a zero-length match must stop at the
-- end of the action list. sequenceMatchEvents has no conditions_met early-out, so an event
-- list that stayed empty reaches the trailing actions with no events at all.
select [] = sequenceMatchEvents('')(t, c = 1, c = 2) from values('t UInt32, c UInt8', (0, 0), (1, 0));
select 1 = sequenceMatch('(?t<=10)')(t, c = 1, c = 2) from values('t UInt32, c UInt8', (0, 0), (1, 0));
-- The events can also run out part way through the pattern, leaving a trailing `.*`.
select 1 = sequenceCount('(?1).*')(t, c = 1, c = 2) from values('t UInt32, c UInt8', (0, 1));
-- A trailing action that a zero-length match does not satisfy still has to fail.
select 0 = sequenceMatch('(?1)(?t<=10)(?2)')(t, c = 1, c = 2) from values('t UInt32, c UInt8', (0, 1), (100, 2));
