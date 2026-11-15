set (CLICKHOUSE_CHECK_MARKS_SOURCES CheckMarks.cpp)

set (CLICKHOUSE_CHECK_MARKS_LINK
    PRIVATE
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        dbms
)

clickhouse_program_add(check-marks)
