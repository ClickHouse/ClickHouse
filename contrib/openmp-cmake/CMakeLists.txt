# Add cmake directory to search for custom cmake functions.
# set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake ${CMAKE_MODULE_PATH})
set(OPENMP_STANDALONE_BUILD ON)
set(ENABLE_LIBOMPTARGET OFF)
set(LIBOMP_USE_VERSION_SYMBOLS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
set (OPENMP_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/openmp/openmp")
set (OPENMP_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/openmp/openmp")
add_subdirectory ("${OPENMP_SOURCE_DIR}" "${OPENMP_BINARY_DIR}")

add_library (_openmp INTERFACE)
target_link_libraries (_openmp INTERFACE omp)
# target_include_directories (_llvm SYSTEM BEFORE INTERFACE ${LLVM_INCLUDE_DIRS})
add_library(ch_contrib::openmp ALIAS _openmp)