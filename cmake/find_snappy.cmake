option(USE_INTERNAL_SNAPPY_LIBRARY "Set to FALSE to use system snappy library instead of bundled" ${NOT_UNBUNDLED})

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/snappy/snappy.h")
   if(USE_INTERNAL_SNAPPY_LIBRARY)
       message(WARNING "submodule contrib/snappy is missing. to fix try run: \n git submodule update --init --recursive")
       set(USE_INTERNAL_SNAPPY_LIBRARY 0)
    endif()
    set(MISSING_INTERNAL_SNAPPY_LIBRARY 1)
endif()

if(NOT USE_INTERNAL_SNAPPY_LIBRARY)
    find_library(SNAPPY_LIBRARY snappy)
    find_path(SNAPPY_INCLUDE_DIR NAMES snappy.h PATHS ${SNAPPY_INCLUDE_PATHS})
endif()

if(SNAPPY_LIBRARY AND SNAPPY_INCLUDE_DIR)
elseif(NOT MISSING_INTERNAL_SNAPPY_LIBRARY)
    set(SNAPPY_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/snappy)
    set(USE_INTERNAL_SNAPPY_LIBRARY 1)
    set(SNAPPY_LIBRARY snappy)
endif()

if(SNAPPY_LIBRARY AND SNAPPY_INCLUDE_DIR)
    set(USE_SNAPPY 1)
endif()

message(STATUS "Using snappy=${USE_SNAPPY}: ${SNAPPY_INCLUDE_DIR} : ${SNAPPY_LIBRARY}")
