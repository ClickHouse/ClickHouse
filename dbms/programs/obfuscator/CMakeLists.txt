add_library (clickhouse-obfuscator-lib ${LINK_MODE} Obfuscator.cpp)
target_link_libraries (clickhouse-obfuscator-lib dbms ${Boost_PROGRAM_OPTIONS_LIBRARY})

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-obfuscator clickhouse-obfuscator.cpp)
    set_target_properties(clickhouse-obfuscator PROPERTIES RUNTIME_OUTPUT_DIRECTORY ..)
    target_link_libraries (clickhouse-obfuscator clickhouse-obfuscator-lib)
endif ()
