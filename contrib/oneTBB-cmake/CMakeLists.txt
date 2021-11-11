set(oneTBB_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/oneTBB-cmake")
set(oneTBB_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/oneTBB")

set(BUILD_SHARED_LIBS OFF CACHE INTERNAL "" FORCE) # actually will use zlib, but skip find
set(CMAKE_CXX_STANDARD 20 CACHE INTERNAL "" FORCE)
set(TBBMALLOC_BUILD OFF CACHE INTERNAL "" FORCE)
set(TBB_OUTPUT_DIR_BASE build CACHE INTERNAL "" FORCE)

add_subdirectory("${oneTBB_SOURCE_DIR}" "${oneTBB_LIBRARY_DIR}")
