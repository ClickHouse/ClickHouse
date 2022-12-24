add_subdirectory(StringUtils)

if (ENABLE_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_MYSQL)
    add_subdirectory (mysqlxx)
endif ()

# Otherwise stack traces symbolization is painfully slow in debug build.
set_source_files_properties("Dwarf.cpp" PROPERTIES COMPILE_FLAGS "-O3")
