# -DENABLE_JEMALLOC=0 -DUSE_STATIC_LIBRARIES=0 -DSPLIT_SHARED_LIBRARIES=1 -DDISABLE_HERMETIC_BUILD=1

option (ENABLE_CLICKHOUSE_PYTHON_MODULE "Python module" ${ENABLE_CLICKHOUSE_ALL})
set (ENABLE_CLICKHOUSE_PYTHON_MODULE ON)

if (ENABLE_CLICKHOUSE_PYTHON_MODULE)

set (ENABLE_JEMALLOC 0)

add_subdirectory(pybind11)
find_package(PythonLibs REQUIRED)
include_directories(${PYTHON_INCLUDE_DIRS})

pybind11_add_module(mylib mylib.cpp)
target_link_libraries(mylib PRIVATE readpassphrase dbms clickhouse_storages_system ${PYTHON_LIBRARIES})
target_include_directories(mylib SYSTEM PUBLIC ${ClickHouse_SOURCE_DIR}/src)
target_include_directories(mylib SYSTEM PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/fmtlib/include)


endif()
