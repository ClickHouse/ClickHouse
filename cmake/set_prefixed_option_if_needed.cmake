macro(set_prefixed_option_if_needed OPTION DEFAULT)

    if (NOT ${DEFAULT} AND ${OPTION} AND NOT CLICKHOUSE_${OPTION})
        message(WARNING "Deprecated option ${OPTION} was specified, use CLICKHOUSE_${OPTION} instead.")
        set(CLICKHOUSE_${OPTION} ON)
    endif()

    if (${DEFAULT} AND NOT ${OPTION} AND CLICKHOUSE_${OPTION})
        message(WARNING "Deprecated option ${OPTION} was specified, use CLICKHOUSE_${OPTION} instead.")
        set(CLICKHOUSE_${OPTION} OFF)
    endif()

endmacro()
