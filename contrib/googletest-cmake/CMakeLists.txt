set (SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/googletest/googletest")

add_library(_gtest "${SRC_DIR}/src/gtest-all.cc")
set_target_properties(_gtest PROPERTIES VERSION "1.0.0")
target_compile_definitions (_gtest INTERFACE GTEST_HAS_POSIX_RE=0)
target_include_directories(_gtest SYSTEM PUBLIC "${SRC_DIR}/include")
target_include_directories(_gtest PRIVATE "${SRC_DIR}")

add_library(_gtest_main "${SRC_DIR}/src/gtest_main.cc")
set_target_properties(_gtest_main PROPERTIES VERSION "1.0.0")
target_link_libraries(_gtest_main PUBLIC _gtest)

add_library(_gtest_all INTERFACE)
target_link_libraries(_gtest_all INTERFACE _gtest _gtest_main)
add_library(ch_contrib::gtest_all ALIAS _gtest_all)
