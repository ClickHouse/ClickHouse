cmake_minimum_required(VERSION 3.0)
project(mimalloc-test C CXX)

# Set default build type
if (NOT CMAKE_BUILD_TYPE)
  if ("${CMAKE_BINARY_DIR}" MATCHES ".*(D|d)ebug$")
    message(STATUS "No build type selected, default to *** Debug ***")
    set(CMAKE_BUILD_TYPE "Debug")
  else()
    message(STATUS "No build type selected, default to *** Release ***")
    set(CMAKE_BUILD_TYPE "Release")
  endif()
endif()

# Import mimalloc (if installed)
find_package(mimalloc 1.0 REQUIRED)

# Tests
add_executable(static-override  main-override.c)
target_link_libraries(static-override PUBLIC mimalloc-static)

add_executable(static-override-cxx  main-override.cpp)
target_link_libraries(static-override-cxx PUBLIC mimalloc-static)

add_executable(dynamic-override  main-override.c)
target_link_libraries(dynamic-override PUBLIC mimalloc)

add_executable(dynamic-override-cxx  main-override.cpp)
target_link_libraries(dynamic-override-cxx PUBLIC mimalloc)
