-- { echoOn }

-- The SESSION frame is experimental and disabled by default.
select 1 n, count() over (order by n session 1); -- { serverError SUPPORT_IS_DISABLED }

set allow_experimental_session_window_frame = 1;

select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n session 1);
select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n session 2);
select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n session 100);
select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n desc session 2);
select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n desc session 100);

-- Fractional session window thresholds are also useful, e.g. to process bursts of events occurring less than 0.5 second apart.
select arrayJoin([1, 2.0, 2.1, 2.2, 10.0, 10.1])::float n, groupArray(n) over (order by n session 0.5);

-- A Float32 key takes a fractional threshold that is not exactly representable in it, the same
-- inexact-float conversion a RANGE offset uses.
select n, groupArray(n) over (order by n session 0.1) from (select arrayJoin([1.0, 1.05, 9.0])::Float32 n) order by n;
select n, count() over (order by n session 0) from (select toFloat32(1) n); -- { serverError BAD_ARGUMENTS }
select n, count() over (order by n session -0.5) from (select toFloat32(1) n); -- { serverError BAD_ARGUMENTS }

-- PARTITION BY: sessions are formed independently within each partition.
select p, n, groupArray(n) over (partition by p order by n session 2)
from (select arrayJoin([1, 2, 10, 11]) n, arrayJoin(['a', 'b']) p)
order by p, n;

-- DateTime ORDER BY key: the threshold is in seconds (RANGE OFFSET units), so
-- SESSION 60 groups events at most 60 seconds apart.
select t, groupArray(t) over (order by t session 60)
from (select arrayJoin([
    toDateTime('2020-01-01 00:00:00'),
    toDateTime('2020-01-01 00:00:30'),
    toDateTime('2020-01-01 00:01:15'),
    toDateTime('2020-01-01 00:05:00')]) t)
order by t;

-- Sessions are formed identically no matter how the input is split into blocks: a gap that
-- falls on a block boundary must still start a new session. Every session below holds
-- exactly 10 rows, so a merged pair shows up as a length of 20.
drop table if exists 02891_session_window_blocks;
create table 02891_session_window_blocks (n UInt64) engine = MergeTree order by n;
insert into 02891_session_window_blocks select number + 1000 * intDiv(number, 10) from numbers(3000);
select length(s) from (select groupArray(n) over (order by n session 2) as s from 02891_session_window_blocks) group by length(s) order by length(s) settings max_block_size = 1;
select length(s) from (select groupArray(n) over (order by n desc session 2) as s from 02891_session_window_blocks) group by length(s) order by length(s) settings max_block_size = 3;
drop table 02891_session_window_blocks;

-- Two windows that differ only in threshold must get different projection names, otherwise
-- they collide into one output column name (and duplicate JSON keys).
-- The threshold is a child of the window definition, so generic traversal and tree hashing
-- see it. Without that the literal is missing from the AST and two windows differing only in
-- threshold compare equal.
explain ast select groupArray(n) over (order by n session 2) from t;

-- Colliding names make this CREATE fail outright with ILLEGAL_COLUMN.
create view 02891_session_window_names as select arrayJoin([1, 20]) n, groupArray(n) over (order by n session 1), groupArray(n) over (order by n session 2);
select name from system.columns where database = currentDatabase() and table = '02891_session_window_names' order by position;
drop view 02891_session_window_names;

-- The threshold type is validated the same way on both analyzer paths.
select 1 n, count() over (order by n session '1') settings enable_analyzer = 1; -- { serverError BAD_ARGUMENTS }
select 1 n, count() over (order by n session '1') settings enable_analyzer = 0; -- { serverError BAD_ARGUMENTS }
-- A Decimal threshold is not a native number, exactly as for a RANGE OFFSET expression.
select n, count() over (order by n session toDecimal64(0.5, 1)) from (select toFloat32(1) n) settings enable_analyzer = 1; -- { serverError BAD_ARGUMENTS }
select n, count() over (order by n session toDecimal64(0.5, 1)) from (select toFloat32(1) n) settings enable_analyzer = 0; -- { serverError BAD_ARGUMENTS }

-- The threshold survives AST JSON serialization, and it is mutually exclusive with the
-- frame boundary fields in both directions.
select formatQueryFromJSON(parseQueryToJSON('SELECT groupArray(n) OVER (ORDER BY n SESSION 2) FROM t'));
select formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Function","name":"groupArray","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"n"}]},"window_definition":{"type":"WindowDefinition","frame_is_default":false,"frame_type":"SESSION","frame_begin_type":"Current","session_window_threshold":{"type":"Literal","value":{"field_type":"UInt64","value":2}}},"is_window_function":true,"kind":"WINDOW_FUNCTION"}]}}]}}'); -- { serverError BAD_ARGUMENTS }
select formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Function","name":"groupArray","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"n"}]},"window_definition":{"type":"WindowDefinition","frame_is_default":false,"frame_type":"SESSION"},"is_window_function":true,"kind":"WINDOW_FUNCTION"}]}}]}}'); -- { serverError BAD_ARGUMENTS }
select formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Function","name":"groupArray","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"n"}]},"window_definition":{"type":"WindowDefinition","frame_is_default":false,"frame_type":"RANGE","frame_begin_type":"Unbounded","frame_begin_preceding":true,"frame_end_type":"Current","frame_end_preceding":false,"session_window_threshold":{"type":"Literal","value":{"field_type":"UInt64","value":2}}},"is_window_function":true,"kind":"WINDOW_FUNCTION"}]}}]}}'); -- { serverError BAD_ARGUMENTS }

-- The threshold also survives query plan serialization, so a distributed plan forms the same
-- sessions as local execution (without it the worker rejects the frame type outright).
drop table if exists 02891_session_window_mt;
create table 02891_session_window_mt (n UInt64) engine = MergeTree order by n;
insert into 02891_session_window_mt values (1), (20), (22), (24), (100), (101);
set make_distributed_plan = 1, serialize_query_plan = 1, distributed_plan_execute_locally = 1;
select n, groupArray(n) over (order by n session 2) from 02891_session_window_mt order by n;
select n, groupArray(n) over (order by n session 100) from 02891_session_window_mt order by n;
set make_distributed_plan = 0, serialize_query_plan = 0, distributed_plan_execute_locally = 0;
drop table 02891_session_window_mt;

-- A window may be named after a frame keyword, including in the parenthesized form.
select number, count() over (session) c from numbers(3) window session as (order by number) order by number;
select number, count() over (session order by number) c from numbers(3) window session as (partition by number % 2) order by number;
select number, count() over (rows) c from numbers(3) window rows as (order by number) order by number;

-- Test some wrong things
select 1 n, count() over (order by n session 0.5); -- { serverError 69 }
select 1 n, count() over (order by n session -1); -- { serverError 69 }
select 1 n, count() over (order by n session 0); -- { serverError 36 }
select 1 n, count() over (order by n session 'what'); -- { serverError BAD_ARGUMENTS }
select 1 n, count() over (session 1); -- { serverError 36 }
select 1 n, count() over (order by n, n+1 session 1); -- { serverError 36 }
select 'a' n, count() over (order by n session 1); -- { serverError 48 }
