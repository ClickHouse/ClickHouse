# limonp is dependency of cppjieba.
# We don't need to make a separate limonp-cmake directory, instead we can add it directly.
set(LIMONP_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/limonp)

add_library(_limonp INTERFACE)
target_include_directories(_limonp SYSTEM INTERFACE ${LIMONP_LIBRARY_DIR}/include)

set(CPPJIEBA_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/cppjieba)

add_library(_cppjieba INTERFACE)
include_directories(${LIMONP_LIBRARY_DIR}/include)

target_include_directories(_cppjieba SYSTEM INTERFACE ${CPPJIEBA_LIBRARY_DIR}/include)
target_link_libraries(_cppjieba INTERFACE _limonp)

add_library(ch_contrib::cppjieba ALIAS _cppjieba)
