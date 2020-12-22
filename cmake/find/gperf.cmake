if(NOT DEFINED ENABLE_GPERF OR ENABLE_GPERF)
    # Check if gperf was installed
    find_program(GPERF gperf)
    if(GPERF)
        option(ENABLE_GPERF "Use gperf function hash generator tool" ${ENABLE_LIBRARIES})
    endif()
endif()

if (ENABLE_GPERF)
    if(NOT GPERF)
        message(FATAL_ERROR "Could not find the program gperf")
    endif()
    set(USE_GPERF 1)
endif()

message(STATUS "Using gperf=${USE_GPERF}: ${GPERF}")
