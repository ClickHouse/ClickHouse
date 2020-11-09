cmake_minimum_required(VERSION 3.7)

project(fuzz)

option(ENABLE_FUZZING "enable building the fuzzers" ON)
set(CMAKE_CXX_STANDARD 17)

if(ENABLE_FUZZING)
  set(MINISELECT_FUZZ_LDFLAGS "" CACHE STRING "LDFLAGS for the fuzz targets")

  add_library(miniselect-fuzzer INTERFACE)
  target_link_libraries(miniselect-fuzzer INTERFACE gtest)
  target_link_libraries(miniselect-fuzzer INTERFACE ${MINISELECT_FUZZ_LDFLAGS})

  if(MINISELECT_FUZZ_LINKMAIN)
      target_sources(simdjson-fuzzer INTERFACE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/main.cpp)
  endif()

  # Define the fuzzers
  add_custom_target(all_fuzzers)

  set(fuzzernames)
  function(implement_fuzzer name)
    add_executable(${name} ${name}.cpp)
    target_link_libraries(${name} PRIVATE miniselect-fuzzer)
    add_dependencies(all_fuzzers ${name})
    set(fuzzernames ${fuzzernames} ${name} PARENT_SCOPE)
  endfunction()

  implement_fuzzer(fuzz_select)
  implement_fuzzer(fuzz_string_select)
  implement_fuzzer(fuzz_sort)
  implement_fuzzer(fuzz_string_sort)

  # to be able to get a list of all fuzzers from within a script
  add_custom_target(print_all_fuzzernames
    COMMAND ${CMAKE_COMMAND} -E echo ${fuzzernames})
endif()
