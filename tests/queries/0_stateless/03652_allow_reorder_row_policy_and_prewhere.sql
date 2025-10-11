-- Tags: no-random-merge-tree-settings

DROP ROW POLICY IF EXISTS test_filter_policy ON test_table;
DROP TABLE IF EXISTS test_table;

create table test_table (light Int32, heavy String codec(NONE)) engine = MergeTree order by tuple();
insert into test_table select number, toString(range(number % 100)) || '_' || if(number % 2, 'aaa', 'bbb') from numbers(1e6);

CREATE ROW POLICY test_filter_policy ON test_table USING (heavy like '%_aaa') TO ALL;

set optimize_move_to_prewhere=1, enable_multiple_prewhere_read_steps=1, allow_reorder_prewhere_conditions=1;

explain actions = 1 select * from test_table where light >= 100000 and light < 100500 settings allow_reorder_row_policy_and_prewhere=0;

explain actions = 1 select * from test_table where light >= 100000 and light < 100500 settings allow_reorder_row_policy_and_prewhere=1;

explain actions = 1 select count() from test_table where throwIf(light % 2 = 0) = 0 and light >= 100000 and light < 100500 settings allow_reorder_row_policy_and_prewhere=1;

select count() from test_table where throwIf(light % 2 = 0) = 0 and light >= 100000 and light < 100500 settings allow_reorder_row_policy_and_prewhere=1;

DROP ROW POLICY IF EXISTS test_filter_policy ON test_table;
DROP TABLE IF EXISTS test_table;
