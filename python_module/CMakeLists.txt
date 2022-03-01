
add_subdirectory(pybind11)

find_package (Python COMPONENTS Interpreter Development)

find_package(PythonLibs REQUIRED)
include_directories(${PYTHON_INCLUDE_DIRS})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-poison-system-directories")

pybind11_add_module(mylib mylib.cpp)
target_link_libraries(mylib PRIVATE readpassphrase dbms ${PYTHON_LIBRARIES})
target_include_directories(mylib SYSTEM PUBLIC ${ClickHouse_SOURCE_DIR}/src)
