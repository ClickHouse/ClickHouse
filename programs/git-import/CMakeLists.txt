set (CLICKHOUSE_GIT_IMPORT_SOURCES git-import.cpp)

set (CLICKHOUSE_GIT_IMPORT_LINK
    PRIVATE
        boost::program_options
        dbms
)

clickhouse_program_add(git-import)

