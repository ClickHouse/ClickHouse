SELECT visibleWidth('ClickHouse是一个很好的数据库');
SELECT visibleWidth('ClickHouse是一个很好的数据库') SETTINGS function_visible_width_behavior = 0;
SELECT visibleWidth('ClickHouse是一个很好的数据库') SETTINGS function_visible_width_behavior = 1;
SELECT visibleWidth('ClickHouse是一个很好的数据库') SETTINGS function_visible_width_behavior = 2; -- { serverError BAD_ARGUMENTS }
SELECT visibleWidth('ClickHouse是一个很好的数据库') SETTINGS compatibility = '23.12';
SELECT visibleWidth('ClickHouse是一个很好的数据库') SETTINGS compatibility = '24.1';
