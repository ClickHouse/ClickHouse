set (CLICKHOUSE_WEB_SERVER_EXPORTER_SOURCES web-server-exporter.cpp)

set (CLICKHOUSE_WEB_SERVER_EXPORTER_LINK
    PRIVATE
        boost::program_options
        dbms
)

clickhouse_program_add(web-server-exporter)
