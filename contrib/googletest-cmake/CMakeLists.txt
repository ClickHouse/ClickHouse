set (SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/googletest/googletest")

add_library(gtest "${SRC_DIR}/src/gtest-all.cc")
set_target_properties(gtest PROPERTIES VERSION "1.0.0")
target_compile_definitions (gtest INTERFACE GTEST_HAS_POSIX_RE=0)
target_include_directories(gtest SYSTEM PUBLIC "${SRC_DIR}/include")
target_include_directories(gtest PRIVATE "${SRC_DIR}")

add_library(gtest_main "${SRC_DIR}/src/gtest_main.cc")
set_target_properties(gtest_main PROPERTIES VERSION "1.0.0")
target_link_libraries(gtest_main PUBLIC gtest)
