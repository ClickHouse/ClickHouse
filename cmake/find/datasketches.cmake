option (ENABLE_DATASKETCHES "Enable DataSketches" ${ENABLE_LIBRARIES})

if (ENABLE_DATASKETCHES)

option (USE_INTERNAL_DATASKETCHES_LIBRARY "Set to FALSE to use system DataSketches library instead of bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/datasketches-cpp/theta/CMakeLists.txt")
    if (USE_INTERNAL_DATASKETCHES_LIBRARY)
       message(WARNING "submodule contrib/datasketches-cpp is missing. to fix try run: \n git submodule update --init")
    endif()
    set(MISSING_INTERNAL_DATASKETCHES_LIBRARY 1)
    set(USE_INTERNAL_DATASKETCHES_LIBRARY 0)
endif()

if (USE_INTERNAL_DATASKETCHES_LIBRARY)
    set(DATASKETCHES_LIBRARY theta)
    set(DATASKETCHES_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/datasketches-cpp/common/include" "${ClickHouse_SOURCE_DIR}/contrib/datasketches-cpp/theta/include")
elseif (NOT MISSING_INTERNAL_DATASKETCHES_LIBRARY)
    find_library(DATASKETCHES_LIBRARY theta)
    find_path(DATASKETCHES_INCLUDE_DIR NAMES theta_sketch.hpp PATHS ${DATASKETCHES_INCLUDE_PATHS})
endif()

if (DATASKETCHES_LIBRARY AND DATASKETCHES_INCLUDE_DIR)
    set(USE_DATASKETCHES 1)
endif()

endif()

message (STATUS "Using datasketches=${USE_DATASKETCHES}: ${DATASKETCHES_INCLUDE_DIR} : ${DATASKETCHES_LIBRARY}")
