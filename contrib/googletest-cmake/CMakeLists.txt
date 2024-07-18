set (SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/googletest")

add_library(_gtest "${SRC_DIR}/googletest/src/gtest-all.cc")
set_target_properties(_gtest PROPERTIES VERSION "1.0.0")
target_compile_definitions (_gtest PUBLIC GTEST_HAS_POSIX_RE=0)
target_include_directories(_gtest SYSTEM PUBLIC "${SRC_DIR}/googletest/include")
target_include_directories(_gtest PRIVATE "${SRC_DIR}/googletest")

add_library(_gtest_main "${SRC_DIR}/googletest/src/gtest_main.cc")
set_target_properties(_gtest_main PROPERTIES VERSION "1.0.0")
target_link_libraries(_gtest_main PUBLIC _gtest)

add_library(_gtest_all INTERFACE)
target_link_libraries(_gtest_all INTERFACE _gtest _gtest_main)
add_library(ch_contrib::gtest_all ALIAS _gtest_all)


add_library(_gmock "${SRC_DIR}/googlemock/src/gmock-all.cc")
set_target_properties(_gmock PROPERTIES VERSION "1.0.0")
target_compile_definitions (_gmock PUBLIC GTEST_HAS_POSIX_RE=0)
target_include_directories(_gmock SYSTEM PUBLIC "${SRC_DIR}/googlemock/include" "${SRC_DIR}/googletest/include")
target_include_directories(_gmock PRIVATE "${SRC_DIR}/googlemock")

add_library(_gmock_main "${SRC_DIR}/googlemock/src/gmock_main.cc")
set_target_properties(_gmock_main PROPERTIES VERSION "1.0.0")
target_link_libraries(_gmock_main PUBLIC _gmock)

add_library(_gmock_all INTERFACE)
target_link_libraries(_gmock_all INTERFACE _gmock _gmock_main)
add_library(ch_contrib::gmock_all ALIAS _gmock_all)
