set (CLICKHOUSE_INSTALL_SOURCES Install.cpp)

# Track dependencies for #embed directives so that changes to embedded files trigger recompilation.
set_source_files_properties(Install.cpp PROPERTIES OBJECT_DEPENDS
    "${CMAKE_SOURCE_DIR}/programs/server/config.xml;\
${CMAKE_SOURCE_DIR}/programs/server/users.xml")

set (CLICKHOUSE_INSTALL_LINK
    PRIVATE
        boost::program_options
        clickhouse_common_config
        clickhouse_common_io
        dbms
        readpassphrase
)

clickhouse_program_add_library(install)
