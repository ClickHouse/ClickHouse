select number, intDiv(1, number - 2) from numbers(4) array join range(number) as x where number != 2 settings enable_lazy_columns_replication=1, query_plan_filter_push_down=0;
