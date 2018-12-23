# Usage:
# set (MAX_COMPILER_MEMORY 2000 CACHE INTERNAL "") # In megabytes
# set (MAX_LINKER_MEMORY 3500 CACHE INTERNAL "")
# include (cmake/limit_jobs.cmake)

cmake_host_system_information(RESULT AVAILABLE_PHYSICAL_MEMORY QUERY AVAILABLE_PHYSICAL_MEMORY) # Not available under freebsd

option(PARALLEL_COMPILE_JOBS "Define the maximum number of concurrent compilation jobs" "")
if (NOT PARALLEL_COMPILE_JOBS AND AVAILABLE_PHYSICAL_MEMORY)
    math(EXPR PARALLEL_COMPILE_JOBS ${AVAILABLE_PHYSICAL_MEMORY}/2500) # ~2.5gb max per one compiler
    if (NOT PARALLEL_COMPILE_JOBS)
        set (PARALLEL_COMPILE_JOBS 1)
    endif ()
endif ()
if (PARALLEL_COMPILE_JOBS)
    set_property(GLOBAL APPEND PROPERTY JOB_POOLS compile_job_pool=${PARALLEL_COMPILE_JOBS})
    set(CMAKE_JOB_POOL_COMPILE compile_job_pool)
endif ()

option(PARALLEL_LINK_JOBS "Define the maximum number of concurrent link jobs" "")
if (NOT PARALLEL_LINK_JOBS AND AVAILABLE_PHYSICAL_MEMORY)
    math(EXPR PARALLEL_LINK_JOBS ${AVAILABLE_PHYSICAL_MEMORY}/4000) # ~4gb max per one linker
    if (NOT PARALLEL_LINK_JOBS)
        set (PARALLEL_LINK_JOBS 1)
    endif ()
endif ()
if (PARALLEL_COMPILE_JOBS OR PARALLEL_LINK_JOBS)
    message(STATUS "Have ${AVAILABLE_PHYSICAL_MEMORY} megabytes of memory. Limiting concurrent linkers jobs to ${PARALLEL_LINK_JOBS} and compiler jobs to ${PARALLEL_COMPILE_JOBS}")
endif ()

if (LLVM_PARALLEL_LINK_JOBS)
    set_property(GLOBAL APPEND PROPERTY JOB_POOLS link_job_pool=${PARALLEL_LINK_JOBS})
    set(CMAKE_JOB_POOL_LINK link_job_pool)
endif ()

