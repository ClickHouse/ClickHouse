option (ENABLE_SIMDJSON "Use simdjson library (internal only)" ${ENABLE_LIBRARIES})

if (ENABLE_SIMDJSON)
    set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/simdjson")

    add_library (simdjson ${LIBRARY_DIR}/src/simdjson.cpp)

    target_include_directories(simdjson
        PUBLIC
            ${LIBRARY_DIR}/include

        PRIVATE
            ${LIBRARY_DIR}/src
    )

    target_compile_definitions(simdjson PUBLIC USE_SIMDJSON=1)

    message (STATUS "Using simdjson")
else ()
    add_library (simdjson INTERFACE)

    target_compile_definitions(simdjson INTERFACE USE_SIMDJSON=0)

    message (STATUS "Not using simdjson")
endif ()
