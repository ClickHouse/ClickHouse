clickhouse_add_executable(test-connect test_connect.cpp)
target_link_libraries (test-connect PRIVATE dbms)


clickhouse_add_executable(preprocess-for-autocomplete preprocess_for_autocomplete.cpp)
target_link_libraries (preprocess-for-autocomplete PRIVATE dbms)

add_custom_command(TARGET preprocess-for-autocomplete POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_SOURCE_DIR}/src/Client/examples/ggml-model-f32.bin"
        $<TARGET_FILE_DIR:preprocess-for-autocomplete>)