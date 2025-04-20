if (ENABLE_TESTS)
    add_subdirectory(tests)
endif()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()

find_package(LLVM REQUIRED CONFIG)

include_directories(SYSTEM ${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})
llvm_map_components_to_libnames(LLVM_LIBS object support xray irreader debuginfo)

target_link_libraries(clickhouse_interpreter PRIVATE ${LLVM_LIBS})
