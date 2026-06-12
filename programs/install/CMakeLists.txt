set (CLICKHOUSE_INSTALL_SOURCES Install.cpp)

set (CLICKHOUSE_INSTALL_LINK
    PRIVATE
        boost::program_options
        clickhouse_common_config
        clickhouse_common_io
        dbms
        readpassphrase
)

clickhouse_program_add_library(install)
