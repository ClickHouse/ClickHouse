add_executable (clickhouse-anonymizer main.cpp)
target_link_libraries (clickhouse-anonymizer dbms ${Boost_PROGRAM_OPTIONS_LIBRARY})
