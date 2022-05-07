# Usage:
# set (MAX_COMPILER_MEMORY 2000 CACHE INTERNAL "") # In megabytes
# set (MAX_LINKER_MEMORY 3500 CACHE INTERNAL "")
# include (cmake/limit_jobs.cmake)

cmake_host_system_information(RESULT AVAILABLE_PHYSICAL_MEMORY QUERY AVAILABLE_PHYSICAL_MEMORY) # Not available under freebsd
cmake_host_system_information(RESULT NUMBER_OF_LOGICAL_CORES QUERY NUMBER_OF_LOGICAL_CORES)

# 1 if not set
option(PARALLEL_COMPILE_JOBS "Maximum number of concurrent compilation jobs" "")

# 1 if not set
option(PARALLEL_LINK_JOBS "Maximum number of concurrent link jobs" "")

if (NOT PARALLEL_COMPILE_JOBS AND AVAILABLE_PHYSICAL_MEMORY AND MAX_COMPILER_MEMORY)
    math(EXPR PARALLEL_COMPILE_JOBS ${AVAILABLE_PHYSICAL_MEMORY}/${MAX_COMPILER_MEMORY})

    if (NOT PARALLEL_COMPILE_JOBS)
        set (PARALLEL_COMPILE_JOBS 1)
    endif ()
endif ()

if (PARALLEL_COMPILE_JOBS AND (NOT NUMBER_OF_LOGICAL_CORES OR PARALLEL_COMPILE_JOBS LESS NUMBER_OF_LOGICAL_CORES))
    set(CMAKE_JOB_POOL_COMPILE compile_job_pool${CMAKE_CURRENT_SOURCE_DIR})
    string (REGEX REPLACE "[^a-zA-Z0-9]+" "_" CMAKE_JOB_POOL_COMPILE ${CMAKE_JOB_POOL_COMPILE})
    set_property(GLOBAL APPEND PROPERTY JOB_POOLS ${CMAKE_JOB_POOL_COMPILE}=${PARALLEL_COMPILE_JOBS})
endif ()


if (NOT PARALLEL_LINK_JOBS AND AVAILABLE_PHYSICAL_MEMORY AND MAX_LINKER_MEMORY)
    math(EXPR PARALLEL_LINK_JOBS ${AVAILABLE_PHYSICAL_MEMORY}/${MAX_LINKER_MEMORY})

    if (NOT PARALLEL_LINK_JOBS)
        set (PARALLEL_LINK_JOBS 1)
    endif ()
endif ()

# ThinLTO provides its own parallel linking
# (which is enabled only for RELWITHDEBINFO)
#
# But use 2 parallel jobs, since:
# - this is what llvm does
# - and I've verfied that lld-11 does not use all available CPU time (in peak) while linking one binary
if (CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" AND ENABLE_THINLTO AND PARALLEL_LINK_JOBS GREATER 2)
    message(STATUS "ThinLTO provides its own parallel linking - limiting parallel link jobs to 2.")
    set (PARALLEL_LINK_JOBS 2)
endif()

if (PARALLEL_LINK_JOBS AND (NOT NUMBER_OF_LOGICAL_CORES OR PARALLEL_LINK_JOBS LESS NUMBER_OF_LOGICAL_CORES))
    set(CMAKE_JOB_POOL_LINK link_job_pool${CMAKE_CURRENT_SOURCE_DIR})
    string (REGEX REPLACE "[^a-zA-Z0-9]+" "_" CMAKE_JOB_POOL_LINK ${CMAKE_JOB_POOL_LINK})
    set_property(GLOBAL APPEND PROPERTY JOB_POOLS ${CMAKE_JOB_POOL_LINK}=${PARALLEL_LINK_JOBS})
endif ()

if (PARALLEL_COMPILE_JOBS OR PARALLEL_LINK_JOBS)
    message(STATUS
        "${CMAKE_CURRENT_SOURCE_DIR}: Have ${AVAILABLE_PHYSICAL_MEMORY} megabytes of memory.
        Limiting concurrent linkers jobs to ${PARALLEL_LINK_JOBS} and compiler jobs to ${PARALLEL_COMPILE_JOBS} (system has ${NUMBER_OF_LOGICAL_CORES} logical cores)")
endif ()
