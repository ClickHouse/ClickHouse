add_definitions(-Wno-unused-variable)
add_definitions(-Wno-deprecated-declarations)

add_library(boost_program_options_internal
boost_1_62_0/libs/program_options/src/cmdline.cpp
boost_1_62_0/libs/program_options/src/config_file.cpp
boost_1_62_0/libs/program_options/src/convert.cpp
boost_1_62_0/libs/program_options/src/options_description.cpp
boost_1_62_0/libs/program_options/src/parsers.cpp
boost_1_62_0/libs/program_options/src/positional_options.cpp
boost_1_62_0/libs/program_options/src/split.cpp
boost_1_62_0/libs/program_options/src/utf8_codecvt_facet.cpp
boost_1_62_0/libs/program_options/src/value_semantic.cpp
boost_1_62_0/libs/program_options/src/variables_map.cpp
boost_1_62_0/libs/program_options/src/winmain.cpp)

add_library(boost_filesystem_internal
boost_1_62_0/libs/filesystem/src/codecvt_error_category.cpp
boost_1_62_0/libs/filesystem/src/operations.cpp
boost_1_62_0/libs/filesystem/src/path.cpp
boost_1_62_0/libs/filesystem/src/path_traits.cpp
boost_1_62_0/libs/filesystem/src/portability.cpp
boost_1_62_0/libs/filesystem/src/unique_path.cpp
boost_1_62_0/libs/filesystem/src/utf8_codecvt_facet.cpp
boost_1_62_0/libs/filesystem/src/windows_file_codecvt.cpp)

add_library(boost_system_internal
boost_1_62_0/libs/system/src/error_code.cpp)

add_library(boost_test_internal
boost_1_62_0/libs/test/src/compiler_log_formatter.cpp
boost_1_62_0/libs/test/src/cpp_main.cpp
boost_1_62_0/libs/test/src/debug.cpp
boost_1_62_0/libs/test/src/decorator.cpp
boost_1_62_0/libs/test/src/execution_monitor.cpp
boost_1_62_0/libs/test/src/framework.cpp
boost_1_62_0/libs/test/src/junit_log_formatter.cpp
boost_1_62_0/libs/test/src/plain_report_formatter.cpp
boost_1_62_0/libs/test/src/progress_monitor.cpp
boost_1_62_0/libs/test/src/results_collector.cpp
boost_1_62_0/libs/test/src/results_reporter.cpp
boost_1_62_0/libs/test/src/test_main.cpp
boost_1_62_0/libs/test/src/test_tools.cpp
boost_1_62_0/libs/test/src/test_tree.cpp
boost_1_62_0/libs/test/src/unit_test_log.cpp
boost_1_62_0/libs/test/src/unit_test_main.cpp
boost_1_62_0/libs/test/src/unit_test_monitor.cpp
boost_1_62_0/libs/test/src/unit_test_parameters.cpp
boost_1_62_0/libs/test/src/xml_log_formatter.cpp
boost_1_62_0/libs/test/src/xml_report_formatter.cpp)

target_include_directories (boost_program_options_internal BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories (boost_filesystem_internal BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories (boost_system_internal BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories (boost_test_internal BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
