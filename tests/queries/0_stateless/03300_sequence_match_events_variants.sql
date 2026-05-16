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
-- First match should be [0,1,2], Last should be [9] (partial), All should include partial
select 'Three consecutive - First' as test, [0,1,2] = sequenceMatchEventsFirst('(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Three consecutive - Last (partial)' as test, [9] = sequenceMatchEventsLast('(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
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
-- data=1 at 10, no data=2 after -> partial match [10]
select 'Mixed pattern - First' as test, [4,5,6] = sequenceMatchEventsFirst('(?2)(?3)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Mixed pattern - Last (partial)' as test, [10] = sequenceMatchEventsLast('(?2)(?3)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test_variants;
select 'Mixed pattern - All count' as test, 2 = length(sequenceMatchEventsAll('(?2)(?3)(?1)')(time, data = 0, data = 1, data = 2, data = 3)) from sequence_test_variants;

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

-- Pattern A->B->C: one complete match [0,1,2], one partial [5,6]
select 'Partial match - First (complete)' as test, [0,1,2] = sequenceMatchEventsFirst('(?1)(?2)(?3)')(time, event='A', event='B', event='C') from sequence_test_variants;
select 'Partial match - Last (partial)' as test, [5,6] = sequenceMatchEventsLast('(?1)(?2)(?3)')(time, event='A', event='B', event='C') from sequence_test_variants;
select 'Partial match - All count' as test, 2 = length(sequenceMatchEventsAll('(?1)(?2)(?3)')(time, event='A', event='B', event='C')) from sequence_test_variants;
select 'Partial match - All complete' as test, [0,1,2] = sequenceMatchEventsAll('(?1)(?2)(?3)')(time, event='A', event='B', event='C')[1] from sequence_test_variants;
select 'Partial match - All partial' as test, [5,6] = sequenceMatchEventsAll('(?1)(?2)(?3)')(time, event='A', event='B', event='C')[2] from sequence_test_variants;

drop table sequence_test_variants;
