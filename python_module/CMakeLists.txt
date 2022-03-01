# -DENABLE_JEMALLOC=0 -DUSE_STATIC_LIBRARIES=0 -DSPLIT_SHARED_LIBRARIES=1 -DDISABLE_HERMETIC_BUILD=1

option (ENABLE_CLICKHOUSE_PYTHON_MODULE "Python module" ${ENABLE_CLICKHOUSE_ALL})
set (ENABLE_CLICKHOUSE_PYTHON_MODULE ON)

if (ENABLE_CLICKHOUSE_PYTHON_MODULE)

set (ENABLE_JEMALLOC 0)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-poison-system-directories")

add_subdirectory(pybind11)
find_package(PythonLibs REQUIRED)
include_directories(${PYTHON_INCLUDE_DIRS})

pybind11_add_module(mylib mylib.cpp)
target_link_libraries(mylib PRIVATE readpassphrase dbms ${PYTHON_LIBRARIES})
target_include_directories(mylib SYSTEM PUBLIC ${ClickHouse_SOURCE_DIR}/src)

endif()
