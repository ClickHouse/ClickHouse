drop table if exists sequence_test;

create table sequence_test (time UInt32, data UInt8) engine=MergeTree ORDER BY tuple();
insert into sequence_test values (0,0),(1,0),(2,0),(3,0),(4,1),(5,2),(6,0),(7,0),(8,0),(9,0),(10,1),(11,1);

select 1 = sequenceMatch('')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('.')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('.*')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceMatch('(?4)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceMatch('(?1)(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?1)(?1)(?1)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?t>10)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceMatch('(?1)(?t>11)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?t<11)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?t<3)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?t<=2)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceMatch('(?1)(?t<2)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?2)(?t>=7)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceMatch('(?2)(?t>7)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?2)(?3)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceMatch('(?1)(?t==2)(?2)')(time, data = 1, data = 2) from sequence_test;
select 1 = sequenceMatch('(?1)(?t==1)(?2)')(time, data = 1, data = 2) from sequence_test;

select count() = sequenceCount('')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select count() = sequenceCount('.')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select count() = sequenceCount('.*')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 8 = sequenceCount('(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 3 = sequenceCount('(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceCount('(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceCount('(?4)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 4 = sequenceCount('(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 2 = sequenceCount('(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 2 = sequenceCount('(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceCount('(?1)(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 2 = sequenceCount('(?1)(?1)(?1)(?1)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceCount('(?1)(?t>10)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceCount('(?1)(?t>11)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 2 = sequenceCount('(?1)(?t<11)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceCount('(?1)(?t<3)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceCount('(?1)(?t<=2)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceCount('(?1)(?t<2)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceCount('(?2)(?t>=7)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceCount('(?2)(?t>7)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceCount('(?2)(?3)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceCount('(?1)(?t==2)(?2)')(time, data = 1, data = 2) from sequence_test;
select 1 = sequenceCount('(?1)(?t==1)(?2)')(time, data = 1, data = 2) from sequence_test;

select [] = sequenceMatchEvents('')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [] = sequenceMatchEvents('.')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [] = sequenceMatchEvents('.*')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [0] = sequenceMatchEvents('(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [4] = sequenceMatchEvents('(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [5] = sequenceMatchEvents('(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [] = sequenceMatchEvents('(?4)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [0,1] = sequenceMatchEvents('(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [0,1,2] = sequenceMatchEvents('(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [0,1,2,3] = sequenceMatchEvents('(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [0,1,2,3] = sequenceMatchEvents('(?1)(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [0,1,2,3,4] = sequenceMatchEvents('(?1)(?1)(?1)(?1)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [0,11] = sequenceMatchEvents('(?1)(?t>10)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [0] = sequenceMatchEvents('(?1)(?t>11)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [0,4] = sequenceMatchEvents('(?1)(?t<11)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [3,5] = sequenceMatchEvents('(?1)(?t<3)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [3,5] = sequenceMatchEvents('(?1)(?t<=2)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [0] =  sequenceMatchEvents('(?1)(?t<2)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [4,11] = sequenceMatchEvents('(?2)(?t>=7)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [4] = sequenceMatchEvents('(?2)(?t>7)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [4,5,6] = sequenceMatchEvents('(?2)(?3)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select [4] = sequenceMatchEvents('(?1)(?t==2)(?2)')(time, data = 1, data = 2) from sequence_test;
select [4,5] = sequenceMatchEvents('(?1)(?t==1)(?2)')(time, data = 1, data = 2) from sequence_test;

-- The returned chain must satisfy the time gate it is matched under. Issue #118120.
select [0, 5] = sequenceMatchEvents('(?1)(?t<=10)(?2)(?3)')(t, e = 'A', e = 'B', e = 'C') from values('t UInt32, e String', (0, 'A'), (5, 'B'), (100, 'B'), (101, 'C'));
select 0 = sequenceMatch('(?1)(?t<=10)(?2)(?3)')(t, e = 'A', e = 'B', e = 'C') from values('t UInt32, e String', (0, 'A'), (5, 'B'), (100, 'B'), (101, 'C'));
select 0 = sequenceCount('(?1)(?t<=10)(?2)(?3)')(t, e = 'A', e = 'B', e = 'C') from values('t UInt32, e String', (0, 'A'), (5, 'B'), (100, 'B'), (101, 'C'));
-- A chain that stops at the gate is still the answer, so this must not shorten to [].
select [0] = sequenceMatchEvents('(?1)(?t<=10)(?2)')(t, e = 'A', e = 'B') from values('t UInt32, e String', (0, 'A'), (100, 'B'));
-- Without a time gate, an event matching a later condition between two matched events breaks the chain.
select [0, 1] = sequenceMatchEvents('(?1)(?2)(?3)')(t, e = 'A', e = 'B', e = 'C') from values('t UInt32, e String', (0, 'A'), (1, 'B'), (2, 'B'), (3, 'C'));
select 0 = sequenceMatch('(?1)(?2)(?3)')(t, e = 'A', e = 'B', e = 'C') from values('t UInt32, e String', (0, 'A'), (1, 'B'), (2, 'B'), (3, 'C'));
-- `.*` still authorises a gap, as documented for sequenceMatchEvents.
select [1, 3, 4] = sequenceMatchEvents('(?1).*(?2).*(?1)(?3)')(time, number = 1, number = 2, number = 4) from values('time UInt32, number UInt32', (1, 1), (2, 3), (3, 2), (4, 1), (5, 3), (6, 2));

-- Event numbers are 1-based; 0 must be rejected, not underflow the condition index.
select sequenceMatch('(?0)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test; -- { serverError BAD_ARGUMENTS }
select sequenceMatch('(?1)(?0)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test; -- { serverError BAD_ARGUMENTS }
select sequenceMatch('(?18446744073709551616)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test; -- { serverError BAD_ARGUMENTS }
select sequenceMatch('(?+)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test; -- { serverError BAD_ARGUMENTS }
select sequenceCount('(?0)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test; -- { serverError BAD_ARGUMENTS }
select sequenceMatchEvents('(?0)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test; -- { serverError BAD_ARGUMENTS }

-- A lone sign in a temporal condition must be rejected, not read as a duration of 0.
select sequenceMatch('(?1)(?t>+)(?2)')(time, data = 1, data = 2) from sequence_test; -- { serverError SYNTAX_ERROR }
select sequenceCount('(?1)(?t<=+)(?2)')(time, data = 1, data = 2) from sequence_test; -- { serverError SYNTAX_ERROR }
select sequenceMatchEvents('(?1)(?t==+)(?2)')(time, data = 1, data = 2) from sequence_test; -- { serverError SYNTAX_ERROR }
select 1 = sequenceMatch('(?1)(?t>+0)(?2)')(time, data = 1, data = 2) from sequence_test;
select 0 = sequenceMatch('(?1)(?t>+1)(?2)')(time, data = 1, data = 2) from sequence_test;

drop table sequence_test;
