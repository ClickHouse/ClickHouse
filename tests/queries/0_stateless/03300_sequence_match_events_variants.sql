-- Test for sequenceMatchEventsFirst, sequenceMatchEventsLast, and sequenceMatchEventsAll functions
drop table if exists sequence_test_variants;

create table sequence_test_variants (time UInt32, data UInt8) engine=MergeTree ORDER BY tuple();

-- Insert data with multiple matching sequences
-- Pattern: data=0 at times 0,1,2,3,6,7,8,9 (two groups of consecutive 0s)
-- Pattern: data=1 at times 4,10,11
-- Pattern: data=2 at time 5
insert into sequence_test_variants values (0,0),(1,0),(2,0),(3,0),(4,1),(5,2),(6,0),(7,0),(8,0),(9,0),(10,1),(11,1);

-- Basic tests: Single event match
-- Should match at time 0 (first), 9 (last), and 8 non-overlapping occurrences (0,1,2,3,6,7,8,9)
select 'Single event - First' as test, [0] = sequenceMatchEventsFirst('(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Single event - Last' as test, [9] = sequenceMatchEventsLast('(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Single event - All count' as test, 8 = length(sequenceMatchEventsAll('(?1)')(time, data = 0, data = 1, data = 2, data = 3)) from sequence_test_variants;

-- Test: Two consecutive events (?1)(?1)
-- Non-overlapping matches: [0,1], [2,3], [6,7], [8,9] = 4 matches
-- First match should be [0,1], Last should be [8,9]
select 'Two consecutive - First' as test, [0,1] = sequenceMatchEventsFirst('(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Two consecutive - Last' as test, [8,9] = sequenceMatchEventsLast('(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Two consecutive - All count' as test, 4 = length(sequenceMatchEventsAll('(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3)) from sequence_test_variants;

-- Test: Three consecutive events (?1)(?1)(?1)
-- Non-overlapping complete matches: [0,1,2], [6,7,8] = 2 matches
-- Partial match at end: [9] (only one data=0 left, can't complete pattern)
-- First should be the first complete match [0,1,2]; Last must prefer the last *complete* match
-- [6,7,8] over the trailing partial [9] - a complete match is always preferred over a partial one.
-- All still reports the trailing partial as its own entry, alongside every complete match.
select 'Three consecutive - First' as test, [0,1,2] = sequenceMatchEventsFirst('(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Three consecutive - Last prefers the last complete match' as test, [6,7,8] = sequenceMatchEventsLast('(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Three consecutive - All count' as test, 3 = length(sequenceMatchEventsAll('(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3)) from sequence_test_variants;
select 'Three consecutive - All has partial' as test, [9] = sequenceMatchEventsAll('(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3)[3] from sequence_test_variants;

-- Test: Four consecutive events (?1)(?1)(?1)(?1)
-- Non-overlapping matches: [0,1,2,3], [6,7,8,9] = 2 matches
select 'Four consecutive - First' as test, [0,1,2,3] = sequenceMatchEventsFirst('(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Four consecutive - Last' as test, [6,7,8,9] = sequenceMatchEventsLast('(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Four consecutive - All count' as test, 2 = length(sequenceMatchEventsAll('(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3)) from sequence_test_variants;

-- Test: Pattern with time constraint (?1)(?t>10)(?2)
-- Only one match possible: event at time 0, then event at time 11 (>10 time units later)
select 'Time constraint >10 - First' as test, [0,11] = sequenceMatchEventsFirst('(?1)(?t>10)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Time constraint >10 - Last' as test, [0,11] = sequenceMatchEventsLast('(?1)(?t>10)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Time constraint >10 - All count' as test, 1 = length(sequenceMatchEventsAll('(?1)(?t>10)(?2)')(time, data = 0, data = 1, data = 2, data = 3)) from sequence_test_variants;

-- Test: Pattern with time constraint (?1)(?t<11)(?2)
-- Possible matches: [0,4], [0,10] - but non-overlapping from position 0
-- First chronologically should be [0,4]
select 'Time constraint <11 - First' as test, [0,4] = sequenceMatchEventsFirst('(?1)(?t<11)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;

-- Test: Mixed pattern (?2)(?3)(?1)
-- data=1 at 4, data=2 at 5, data=0 at 6 -> complete match [4,5,6]
-- data=1 at 10 and 11, no data=2 after either -> only trailing partials remain after that.
-- Last must prefer the complete match [4,5,6] over any trailing partial (a complete match is
-- always preferred). All still reports the latest trailing partial ([11], not [10] - matching
-- only action1/cond2 with nothing left to attempt cond3 against beats [10], which got as far as
-- failing cond3 against 11) as its own entry, alongside the complete match.
select 'Mixed pattern - First' as test, [4,5,6] = sequenceMatchEventsFirst('(?2)(?3)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Mixed pattern - Last prefers the complete match' as test, [4,5,6] = sequenceMatchEventsLast('(?2)(?3)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Mixed pattern - All count' as test, 2 = length(sequenceMatchEventsAll('(?2)(?3)(?1)')(time, data = 0, data = 1, data = 2, data = 3)) from sequence_test_variants;
select 'Mixed pattern - All trailing partial is the latest anchor' as test, [11] = sequenceMatchEventsAll('(?2)(?3)(?1)')(time, data = 0, data = 1, data = 2, data = 3)[2] from sequence_test_variants;

-- Test: No match cases (data=3 never appears)
select 'No match - First' as test, [] = sequenceMatchEventsFirst('(?4)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'No match - Last' as test, [] = sequenceMatchEventsLast('(?4)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'No match - All count' as test, 0 = length(sequenceMatchEventsAll('(?4)')(time, data = 0, data = 1, data = 2, data = 3)) from sequence_test_variants;

-- Test comparing original sequenceMatchEvents (longest) vs new variants
-- For pattern (?1)(?1)(?1)(?1), original returns longest match
-- First should return [0,1,2,3], Last should return [6,7,8,9]
select 'Comparison - Original (longest)' as test, [0,1,2,3] = sequenceMatchEvents('(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Comparison - First' as test, [0,1,2,3] = sequenceMatchEventsFirst('(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Comparison - Last' as test, [6,7,8,9] = sequenceMatchEventsLast('(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;

-- Additional test with more complex data
drop table if exists sequence_test_variants;
create table sequence_test_variants (time UInt32, event String) engine=MergeTree ORDER BY tuple();
insert into sequence_test_variants values
    (0, 'A'),(1, 'B'),(2, 'C'),
    (5, 'A'),(6, 'B'),(7, 'C'),
    (10, 'A'),(11, 'B'),(12, 'C');

-- Pattern A->B->C appears three times: [0,1,2], [5,6,7], [10,11,12]
select 'Multi-occurrence - First' as test, [0,1,2] = sequenceMatchEventsFirst('(?1)(?2)(?3)')(time, event='A', event='B', event='C') from sequence_test_variants;
select 'Multi-occurrence - Last' as test, [10,11,12] = sequenceMatchEventsLast('(?1)(?2)(?3)')(time, event='A', event='B', event='C') from sequence_test_variants;
select 'Multi-occurrence - All count' as test, 3 = length(sequenceMatchEventsAll('(?1)(?2)(?3)')(time, event='A', event='B', event='C')) from sequence_test_variants;

-- Verify all three sequences are captured correctly
select 'Multi-occurrence - All first' as test, sequenceMatchEventsAll('(?1)(?2)(?3)')(time, event='A', event='B', event='C')[1] = [0,1,2] from sequence_test_variants;
select 'Multi-occurrence - All second' as test, sequenceMatchEventsAll('(?1)(?2)(?3)')(time, event='A', event='B', event='C')[2] = [5,6,7] from sequence_test_variants;
select 'Multi-occurrence - All third' as test, sequenceMatchEventsAll('(?1)(?2)(?3)')(time, event='A', event='B', event='C')[3] = [10,11,12] from sequence_test_variants;

-- Test partial match scenarios with incomplete patterns
drop table if exists sequence_test_variants;
create table sequence_test_variants (time UInt32, event String) engine=MergeTree ORDER BY tuple();
insert into sequence_test_variants values
    (0, 'A'),(1, 'B'),(2, 'C'),
    (5, 'A'),(6, 'B');  -- Incomplete pattern

-- Pattern A->B->C: one complete match [0,1,2], one partial [5,6]. Last must prefer the complete
-- match over the trailing partial (a complete match is always preferred over a partial one), even
-- though the partial is chronologically later; All still reports both, in order.
select 'Partial match - First (complete)' as test, [0,1,2] = sequenceMatchEventsFirst('(?1)(?2)(?3)')(time, event='A', event='B', event='C') from sequence_test_variants;
select 'Partial match - Last prefers the complete match' as test, [0,1,2] = sequenceMatchEventsLast('(?1)(?2)(?3)')(time, event='A', event='B', event='C') from sequence_test_variants;
select 'Partial match - All count' as test, 2 = length(sequenceMatchEventsAll('(?1)(?2)(?3)')(time, event='A', event='B', event='C')) from sequence_test_variants;
select 'Partial match - All complete' as test, [0,1,2] = sequenceMatchEventsAll('(?1)(?2)(?3)')(time, event='A', event='B', event='C')[1] from sequence_test_variants;
select 'Partial match - All partial' as test, [5,6] = sequenceMatchEventsAll('(?1)(?2)(?3)')(time, event='A', event='B', event='C')[2] from sequence_test_variants;

-- Test: First must prefer the earliest match over a longer, later one.
-- The leading implicit ".*" in the pattern lets a single backtracking search skip ahead and find a
-- match starting anywhere in the remaining data; without anchoring the search to the current
-- position, First could incorrectly return a longer match found later instead of the true first one.
-- Note: rows matching none of the given conditions are dropped entirely from consideration (see
-- AggregateFunctionSequenceMatchData::add), so the "gap" row must satisfy some *other* passed
-- condition (data=2 here) to stay visible without matching the pattern's own actions.
drop table if exists sequence_test_variants;
create table sequence_test_variants (time UInt32, data UInt8) engine=MergeTree ORDER BY tuple();
insert into sequence_test_variants values (0,0),(1,2),(2,0),(3,0),(4,2);

-- Pattern (?1)(?1)(?2): data=0 at times 0,2,3; data=1 never appears; data=2 (times 1,4) is passed as
-- a condition but not referenced by the pattern, just to keep those rows from being dropped.
-- Anchored at time 0: matches action1, but action2 fails at time 1 (data=2) -> partial [0].
-- Anchored at time 2: matches action1 and action2 (both data=0 at 2,3), but action3 (data=1) never
-- matches -> partial [2,3], which is longer but starts later.
-- First must return the earlier, shorter partial [0], not the longer, later one [2,3].
select 'First prefers earliest over longest' as test, [0] = sequenceMatchEventsFirst('(?1)(?1)(?2)')(time, data = 0, data = 1, data = 2) from sequence_test_variants;

-- Test: First must prefer a later *complete* match over an earlier *partial* one.
-- An earlier anchor producing only a partial match must not shadow a genuine complete match that
-- starts at a later position.
drop table if exists sequence_test_variants;
create table sequence_test_variants (time UInt32, event String) engine=MergeTree ORDER BY tuple();
insert into sequence_test_variants values (1, 'A'), (2, 'A'), (3, 'B');

-- Pattern (?1)(?2): anchored at time 1, action1 (A) matches but action2 (B) fails at time 2 (A) ->
-- partial [1]. The complete match [2,3] (A then B) starts right after. First must return [2,3].
select 'First prefers a later complete match over an earlier partial' as test, [2,3] = sequenceMatchEventsFirst('(?1)(?2)')(time, event = 'A', event = 'B') from sequence_test_variants;

drop table sequence_test_variants;

-- Test: Last must prefer a later partial over an earlier, longer one.
-- No complete match exists anywhere (cond3='C' never occurs). Anchored at time 1: matches action1
-- and action2 (A then B), but action3 (C) fails at time 3 -> partial [1,2], length 2. Anchored at
-- time 3: matches only action1 (A) with nothing left to attempt action2 against -> partial [3],
-- length 1 but starting later. Last must return the later, shorter [3], not the earlier, longer [1,2].
create table sequence_test_variants (time UInt32, event String) engine=MergeTree ORDER BY tuple();
insert into sequence_test_variants values (1, 'A'), (2, 'B'), (3, 'A');

select 'Last prefers a later partial over an earlier, longer one' as test, [3] = sequenceMatchEventsLast('(?1)(?2)(?3)')(time, event = 'A', event = 'B', event = 'C') from sequence_test_variants;

drop table sequence_test_variants;

-- Test: a time constraint scanning forward internally on failure must not cause Last/All to skip
-- later candidate anchors. No complete match exists anywhere (cond2='B' never occurs). Anchored at
-- time 0: action1 (A) matches, then (?t>10) scans forward past times 5 and 20 looking for a
-- satisfying timestamp before giving up -> if the scan resumed from wherever that search left off
-- instead of from the very next candidate position, times 5 and 20 would never be tried on their
-- own, and the wrong (earliest, not latest) partial would be returned.
create table sequence_test_variants (time UInt32, event String) engine=MergeTree ORDER BY tuple();
insert into sequence_test_variants values (0, 'A'), (5, 'A'), (20, 'A');

select 'Last is not fooled by a time constraint scanning past later anchors' as test, [20] = sequenceMatchEventsLast('(?1)(?t>10)(?2)')(time, event = 'A', event = 'B') from sequence_test_variants;
select 'All is not fooled by a time constraint scanning past later anchors' as test, [[20]] = sequenceMatchEventsAll('(?1)(?t>10)(?2)')(time, event = 'A', event = 'B') from sequence_test_variants;

drop table sequence_test_variants;

-- Test: patterns with no numbered (?N) captures at all ('', '.', '.*') are valid, per
-- 00222_sequence_aggregate_function_family.sql, which checks them against sequenceMatch/sequenceCount.
-- They match (or match trivially at every position) but never capture any event, since captured
-- events only come from (?N) actions. First/Last therefore return [] here, same as
-- sequenceMatchEvents('') / ('.') / ('.*') already do (also []) - [] does not distinguish "matched,
-- nothing captured" from "did not match at all". All returns one (empty) array per non-overlapping
-- match, so its length still matches sequenceCount's count for the same pattern.
create table sequence_test_variants (time UInt32, data UInt8) engine=MergeTree ORDER BY tuple();
insert into sequence_test_variants values (0,0),(1,0),(2,0),(3,0),(4,1),(5,2),(6,0),(7,0),(8,0),(9,0),(10,1),(11,1);

select 'Empty pattern - First' as test, [] = sequenceMatchEventsFirst('')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Empty pattern - Last' as test, [] = sequenceMatchEventsLast('')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Empty pattern - All count matches sequenceCount' as test, sequenceCount('')(time, data = 0, data = 1, data = 2, data = 3) = length(sequenceMatchEventsAll('')(time, data = 0, data = 1, data = 2, data = 3)) from sequence_test_variants;
select 'Empty pattern - All entries are empty' as test, [] = sequenceMatchEventsAll('')(time, data = 0, data = 1, data = 2, data = 3)[1] from sequence_test_variants;

select 'Any-event pattern (.) - First' as test, [] = sequenceMatchEventsFirst('.')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Any-event pattern (.) - Last' as test, [] = sequenceMatchEventsLast('.')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Any-event pattern (.) - All count matches sequenceCount' as test, sequenceCount('.')(time, data = 0, data = 1, data = 2, data = 3) = length(sequenceMatchEventsAll('.')(time, data = 0, data = 1, data = 2, data = 3)) from sequence_test_variants;

select 'Kleene pattern (.*) - First' as test, [] = sequenceMatchEventsFirst('.*')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Kleene pattern (.*) - Last' as test, [] = sequenceMatchEventsLast('.*')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Kleene pattern (.*) - All count matches sequenceCount' as test, sequenceCount('.*')(time, data = 0, data = 1, data = 2, data = 3) = length(sequenceMatchEventsAll('.*')(time, data = 0, data = 1, data = 2, data = 3)) from sequence_test_variants;

drop table sequence_test_variants;
