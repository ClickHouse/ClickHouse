# Fuzzers of the Avro read paths: the Rust decoder when `USE_AVRO_RUST` is set (`ch_rust::avro` exists), otherwise the
# C++ one (`contrib/avro` as ClickHouse uses it). `dbms` links `ch_contrib::avrocpp` and `ch_rust::avro` privately, so
# the Avro headers (`avro_rust.h` is reached through `AvroRowInputFormat.h`) need the libraries here as well.
if (TARGET ch_contrib::avrocpp)
    set(AVRO_FUZZER_LIBS clickhouse_aggregate_functions dbms ch_contrib::avrocpp)
    if (TARGET ch_rust::avro)
        list(APPEND AVRO_FUZZER_LIBS ch_rust::avro)
    endif ()

    clickhouse_add_executable(avro_schema_fuzzer avro_schema_fuzzer.cpp AvroFuzzerCommon.cpp)
    target_link_libraries(avro_schema_fuzzer PRIVATE ${AVRO_FUZZER_LIBS})

    clickhouse_add_executable(avro_ocf_fuzzer avro_ocf_fuzzer.cpp AvroFuzzerCommon.cpp)
    target_link_libraries(avro_ocf_fuzzer PRIVATE ${AVRO_FUZZER_LIBS})

    clickhouse_add_executable(avro_confluent_fuzzer avro_confluent_fuzzer.cpp AvroFuzzerCommon.cpp)
    target_link_libraries(avro_confluent_fuzzer PRIVATE ${AVRO_FUZZER_LIBS})

    clickhouse_add_executable(avro_iceberg_manifest_fuzzer avro_iceberg_manifest_fuzzer.cpp AvroFuzzerCommon.cpp)
    target_link_libraries(avro_iceberg_manifest_fuzzer PRIVATE ${AVRO_FUZZER_LIBS})
endif ()
