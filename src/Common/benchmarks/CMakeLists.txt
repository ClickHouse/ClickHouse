clickhouse_add_executable(integer_hash_tables_and_hashes integer_hash_tables_and_hashes.cpp orc_string_dictionary.cpp)
target_link_libraries (integer_hash_tables_and_hashes PRIVATE
    ch_contrib::gbenchmark_all
    dbms
    ch_contrib::abseil_swiss_tables
    ch_contrib::sparsehash
    ch_contrib::wyhash
    ch_contrib::farmhash
    ch_contrib::xxHash)

clickhouse_add_executable(orc_string_dictionary orc_string_dictionary.cpp)
target_link_libraries (orc_string_dictionary PRIVATE
    ch_contrib::gbenchmark_all
    dbms)
