-- Tags: no-async-insert
-- ^ due to the usage of async_insert_deduplicate setting, which is set to true in AsyncInsert test sute

-- { echo }

select value from system.settings where name='allow_settings_after_format_in_insert';
select value from system.settings where name='allow_settings_after_format_in_insert' settings compatibility='22.3';
select value from system.settings where name='allow_settings_after_format_in_insert';
set compatibility = '22.3';
select value from system.settings where name='allow_settings_after_format_in_insert';
set compatibility = '22.4';
select value from system.settings where name='allow_settings_after_format_in_insert';
set allow_settings_after_format_in_insert=1;
select value from system.settings where name='allow_settings_after_format_in_insert';
set compatibility = '22.4';
select value from system.settings where name='allow_settings_after_format_in_insert';
set compatibility = '22.3';
select value from system.settings where name='allow_settings_after_format_in_insert';

set compatibility = '25.12';
select value from system.settings where name='deduplicate_blocks_in_dependent_materialized_views';
select value from system.settings where name='deduplicate_insert';
select value from system.settings where name='insert_deduplicate';
select value from system.settings where name='async_insert_deduplicate';
set compatibility = '26.1';
select value from system.settings where name='deduplicate_blocks_in_dependent_materialized_views';
select value from system.settings where name='deduplicate_insert';
select value from system.settings where name='insert_deduplicate';
select value from system.settings where name='async_insert_deduplicate';
set compatibility = '26.2';
select value from system.settings where name='deduplicate_blocks_in_dependent_materialized_views';
select value from system.settings where name='deduplicate_insert';
select value from system.settings where name='insert_deduplicate';
select value from system.settings where name='async_insert_deduplicate';
