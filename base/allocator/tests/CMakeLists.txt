# Each `*.cpp` file here (except TestMain.cpp) is a separate test executable linked with the allocator internals.
#
# Tests named `*_api.cpp` are end-to-end tests of the public API: they also link Api.cpp, whose constructor initializes
# the allocator (with `MALLOC_CONF` from the environment). They are registered twice: without heap profiling and with
# frequent sampling (`<name>_prof`).
#
# Tests that need the reference jemalloc (an oracle) are named `*_oracle.cpp`; they are only built when
# `ALLOCATOR_REFERENCE_JEMALLOC` is set and may have a companion C translation unit `<name>_ref.c` that includes
# jemalloc's internal headers.

add_library (allocator_test_main STATIC TestMain.cpp)
target_include_directories (allocator_test_main PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")

set (JEMALLOC_SOURCE_DIR "${ALLOCATOR_SOURCES_DIR}/../../contrib/jemalloc")
set (JEMALLOC_CMAKE_DIR "${ALLOCATOR_SOURCES_DIR}/../../contrib/jemalloc-cmake")

file (GLOB TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
list (REMOVE_ITEM TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/TestMain.cpp")

foreach (test_source ${TEST_SOURCES})
    get_filename_component (test_name "${test_source}" NAME_WE)
    set (ref_source "${CMAKE_CURRENT_SOURCE_DIR}/${test_name}_ref.c")

    if (test_name MATCHES "_oracle$")
        if (NOT ALLOCATOR_REFERENCE_JEMALLOC)
            continue ()
        endif ()
        add_executable (${test_name} ${test_source})
        if (EXISTS "${ref_source}")
            target_sources (${test_name} PRIVATE "${ref_source}")
            set_source_files_properties ("${ref_source}" PROPERTIES
                INCLUDE_DIRECTORIES "${JEMALLOC_CMAKE_DIR}/include;${ALLOCATOR_REFERENCE_INCLUDE};${JEMALLOC_SOURCE_DIR}/include"
                COMPILE_DEFINITIONS "JEMALLOC_NO_PRIVATE_NAMESPACE;JEMALLOC_NO_RENAME")
        endif ()
        if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${test_name}.cmake")
            # The test links the reference in its own way (e.g. a patched copy of it).
            include ("${CMAKE_CURRENT_SOURCE_DIR}/${test_name}.cmake")
        else ()
            target_link_libraries (${test_name} PRIVATE "${ALLOCATOR_REFERENCE_JEMALLOC}")
        endif ()
    else ()
        add_executable (${test_name} ${test_source})
    endif ()

    target_link_libraries (${test_name} PRIVATE allocator_test_main _allocator_core)
    target_compile_options (${test_name} PRIVATE -fno-exceptions)
    if (test_name MATCHES "_api$")
        target_link_libraries (${test_name} PRIVATE _allocator)
        set (api_test_conf "background_thread:false,tcache_gc_incr_bytes:2147483648")
        add_test (NAME ${test_name} COMMAND ${test_name})
        set_tests_properties (${test_name} PROPERTIES ENVIRONMENT "MALLOC_CONF=${api_test_conf},prof:false")
        add_test (NAME ${test_name}_prof COMMAND ${test_name})
        set_tests_properties (${test_name}_prof PROPERTIES ENVIRONMENT "MALLOC_CONF=${api_test_conf},prof:true,prof_active:true,prof_thread_active_init:true,lg_prof_sample:14")
    else ()
        add_test (NAME ${test_name} COMMAND ${test_name})
    endif ()
endforeach ()
