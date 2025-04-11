set(CPPJIEBA_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/cppjieba)

add_library(_cppjieba INTERFACE)
target_include_directories(_cppjieba SYSTEM INTERFACE ${CPPJIEBA_LIBRARY_DIR}/include ${CPPJIEBA_LIBRARY_DIR}/deps/limonp/include)

add_library(ch_contrib::cppjieba ALIAS _cppjieba)
