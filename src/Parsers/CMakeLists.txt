if (USE_DEBUG_HELPERS)
    set (INCLUDE_DEBUG_HELPERS "-I\"${ClickHouse_SOURCE_DIR}/base\" -include \"${ClickHouse_SOURCE_DIR}/src/Parsers/iostream_debug_helpers.h\"")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${INCLUDE_DEBUG_HELPERS}")
endif ()

if(ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()
