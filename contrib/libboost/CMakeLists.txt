add_definitions(-Wno-unused-variable)
add_definitions(-Wno-deprecated-declarations)

add_library(boost_program_options_internal
boost_1_65_0/libs/program_options/src/cmdline.cpp
boost_1_65_0/libs/program_options/src/config_file.cpp
boost_1_65_0/libs/program_options/src/convert.cpp
boost_1_65_0/libs/program_options/src/options_description.cpp
boost_1_65_0/libs/program_options/src/parsers.cpp
boost_1_65_0/libs/program_options/src/positional_options.cpp
boost_1_65_0/libs/program_options/src/split.cpp
boost_1_65_0/libs/program_options/src/utf8_codecvt_facet.cpp
boost_1_65_0/libs/program_options/src/value_semantic.cpp
boost_1_65_0/libs/program_options/src/variables_map.cpp
boost_1_65_0/libs/program_options/src/winmain.cpp)

add_library(boost_filesystem_internal
boost_1_65_0/libs/filesystem/src/codecvt_error_category.cpp
boost_1_65_0/libs/filesystem/src/operations.cpp
boost_1_65_0/libs/filesystem/src/path.cpp
boost_1_65_0/libs/filesystem/src/path_traits.cpp
boost_1_65_0/libs/filesystem/src/portability.cpp
boost_1_65_0/libs/filesystem/src/unique_path.cpp
boost_1_65_0/libs/filesystem/src/utf8_codecvt_facet.cpp
boost_1_65_0/libs/filesystem/src/windows_file_codecvt.cpp)

add_library(boost_system_internal
boost_1_65_0/libs/system/src/error_code.cpp)

target_include_directories (boost_program_options_internal BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories (boost_filesystem_internal BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories (boost_system_internal BEFORE PUBLIC ${Boost_INCLUDE_DIRS})

target_compile_definitions (boost_program_options_internal PUBLIC BOOST_SYSTEM_NO_DEPRECATED)
target_compile_definitions (boost_filesystem_internal PUBLIC BOOST_SYSTEM_NO_DEPRECATED)
target_compile_definitions (boost_system_internal PUBLIC BOOST_SYSTEM_NO_DEPRECATED)
