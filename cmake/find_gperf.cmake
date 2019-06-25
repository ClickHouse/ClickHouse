# Check if gperf was installed
find_program(GPERF gperf)
if(GPERF)
    option(ENABLE_GPERF "Use gperf function hash generator tool" ON)
endif()
if (ENABLE_GPERF)
    if(NOT GPERF)
        message(FATAL_ERROR "Could not find the program gperf")
    endif()
    set(USE_GPERF 1)
endif()

message(STATUS "Using gperf=${USE_GPERF}: ${GPERF}")
