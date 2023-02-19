option(ENABLE_PROTOBUF "Enable protobuf" ${ENABLE_LIBRARIES})

if(NOT ENABLE_PROTOBUF)
  message(STATUS "Not using protobuf")
  return()
endif()

set(Protobuf_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/protobuf/src")
if(OS_FREEBSD AND SANITIZE STREQUAL "address")
  # ../contrib/protobuf/src/google/protobuf/arena_impl.h:45:10: fatal error: 'sanitizer/asan_interface.h' file not found
  # #include <sanitizer/asan_interface.h>
  if(LLVM_INCLUDE_DIRS)
    set(Protobuf_INCLUDE_DIR "${Protobuf_INCLUDE_DIR}" ${LLVM_INCLUDE_DIRS})
  else()
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't use protobuf on FreeBSD with address sanitizer without LLVM")
    return()
  endif()
endif()

set(protobuf_source_dir "${ClickHouse_SOURCE_DIR}/contrib/protobuf")
set(protobuf_binary_dir "${ClickHouse_BINARY_DIR}/contrib/protobuf")


add_definitions(-DGOOGLE_PROTOBUF_CMAKE_BUILD)

add_definitions(-DHAVE_PTHREAD)
add_definitions(-DHAVE_ZLIB)

include_directories(
  ${protobuf_binary_dir}
  ${protobuf_source_dir}/src)

set(libprotobuf_lite_files
  ${protobuf_source_dir}/src/google/protobuf/any_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/arena.cc
  ${protobuf_source_dir}/src/google/protobuf/arenastring.cc
  ${protobuf_source_dir}/src/google/protobuf/extension_set.cc
  ${protobuf_source_dir}/src/google/protobuf/field_access_listener.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_enum_util.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_message_table_driven_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_message_util.cc
  ${protobuf_source_dir}/src/google/protobuf/implicit_weak_message.cc
  ${protobuf_source_dir}/src/google/protobuf/io/coded_stream.cc
  ${protobuf_source_dir}/src/google/protobuf/io/io_win32.cc
  ${protobuf_source_dir}/src/google/protobuf/io/strtod.cc
  ${protobuf_source_dir}/src/google/protobuf/io/zero_copy_stream.cc
  ${protobuf_source_dir}/src/google/protobuf/io/zero_copy_stream_impl.cc
  ${protobuf_source_dir}/src/google/protobuf/io/zero_copy_stream_impl_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/map.cc
  ${protobuf_source_dir}/src/google/protobuf/message_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/parse_context.cc
  ${protobuf_source_dir}/src/google/protobuf/repeated_field.cc
  ${protobuf_source_dir}/src/google/protobuf/stubs/bytestream.cc
  ${protobuf_source_dir}/src/google/protobuf/stubs/common.cc
  ${protobuf_source_dir}/src/google/protobuf/stubs/int128.cc
  ${protobuf_source_dir}/src/google/protobuf/stubs/status.cc
  ${protobuf_source_dir}/src/google/protobuf/stubs/statusor.cc
  ${protobuf_source_dir}/src/google/protobuf/stubs/stringpiece.cc
  ${protobuf_source_dir}/src/google/protobuf/stubs/stringprintf.cc
  ${protobuf_source_dir}/src/google/protobuf/stubs/structurally_valid.cc
  ${protobuf_source_dir}/src/google/protobuf/stubs/strutil.cc
  ${protobuf_source_dir}/src/google/protobuf/stubs/time.cc
  ${protobuf_source_dir}/src/google/protobuf/wire_format_lite.cc
)

add_library(_libprotobuf-lite ${libprotobuf_lite_files})
target_link_libraries(_libprotobuf-lite pthread)
if(${CMAKE_SYSTEM_NAME} STREQUAL "Android")
    target_link_libraries(_libprotobuf-lite log)
endif()
target_include_directories(_libprotobuf-lite SYSTEM PUBLIC ${protobuf_source_dir}/src)
add_library(protobuf::libprotobuf-lite ALIAS _libprotobuf-lite)


set(libprotobuf_files
  ${protobuf_source_dir}/src/google/protobuf/any.cc
  ${protobuf_source_dir}/src/google/protobuf/any.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/api.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/importer.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/parser.cc
  ${protobuf_source_dir}/src/google/protobuf/descriptor.cc
  ${protobuf_source_dir}/src/google/protobuf/descriptor.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/descriptor_database.cc
  ${protobuf_source_dir}/src/google/protobuf/duration.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/dynamic_message.cc
  ${protobuf_source_dir}/src/google/protobuf/empty.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/extension_set_heavy.cc
  ${protobuf_source_dir}/src/google/protobuf/field_mask.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_message_reflection.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_message_table_driven.cc
  ${protobuf_source_dir}/src/google/protobuf/io/gzip_stream.cc
  ${protobuf_source_dir}/src/google/protobuf/io/printer.cc
  ${protobuf_source_dir}/src/google/protobuf/io/tokenizer.cc
  ${protobuf_source_dir}/src/google/protobuf/map_field.cc
  ${protobuf_source_dir}/src/google/protobuf/message.cc
  ${protobuf_source_dir}/src/google/protobuf/reflection_ops.cc
  ${protobuf_source_dir}/src/google/protobuf/service.cc
  ${protobuf_source_dir}/src/google/protobuf/source_context.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/struct.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/stubs/substitute.cc
  ${protobuf_source_dir}/src/google/protobuf/text_format.cc
  ${protobuf_source_dir}/src/google/protobuf/timestamp.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/type.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/unknown_field_set.cc
  ${protobuf_source_dir}/src/google/protobuf/util/delimited_message_util.cc
  ${protobuf_source_dir}/src/google/protobuf/util/field_comparator.cc
  ${protobuf_source_dir}/src/google/protobuf/util/field_mask_util.cc
  ${protobuf_source_dir}/src/google/protobuf/util/internal/datapiece.cc
  ${protobuf_source_dir}/src/google/protobuf/util/internal/default_value_objectwriter.cc
  ${protobuf_source_dir}/src/google/protobuf/util/internal/error_listener.cc
  ${protobuf_source_dir}/src/google/protobuf/util/internal/field_mask_utility.cc
  ${protobuf_source_dir}/src/google/protobuf/util/internal/json_escaping.cc
  ${protobuf_source_dir}/src/google/protobuf/util/internal/json_objectwriter.cc
  ${protobuf_source_dir}/src/google/protobuf/util/internal/json_stream_parser.cc
  ${protobuf_source_dir}/src/google/protobuf/util/internal/object_writer.cc
  ${protobuf_source_dir}/src/google/protobuf/util/internal/proto_writer.cc
  ${protobuf_source_dir}/src/google/protobuf/util/internal/protostream_objectsource.cc
  ${protobuf_source_dir}/src/google/protobuf/util/internal/protostream_objectwriter.cc
  ${protobuf_source_dir}/src/google/protobuf/util/internal/type_info.cc
  ${protobuf_source_dir}/src/google/protobuf/util/internal/type_info_test_helper.cc
  ${protobuf_source_dir}/src/google/protobuf/util/internal/utility.cc
  ${protobuf_source_dir}/src/google/protobuf/util/json_util.cc
  ${protobuf_source_dir}/src/google/protobuf/util/message_differencer.cc
  ${protobuf_source_dir}/src/google/protobuf/util/time_util.cc
  ${protobuf_source_dir}/src/google/protobuf/util/type_resolver_util.cc
  ${protobuf_source_dir}/src/google/protobuf/wire_format.cc
  ${protobuf_source_dir}/src/google/protobuf/wrappers.pb.cc
)

add_library(_libprotobuf ${libprotobuf_lite_files} ${libprotobuf_files})
if (ENABLE_FUZZING)
    target_compile_options(_libprotobuf PRIVATE "-fsanitize-recover=all")
endif()
target_link_libraries(_libprotobuf pthread)
target_link_libraries(_libprotobuf ch_contrib::zlib)
if(${CMAKE_SYSTEM_NAME} STREQUAL "Android")
    target_link_libraries(_libprotobuf log)
endif()
target_include_directories(_libprotobuf SYSTEM PUBLIC ${protobuf_source_dir}/src)
add_library(protobuf::libprotobuf ALIAS _libprotobuf)


set(libprotoc_files
  ${protobuf_source_dir}/src/google/protobuf/compiler/code_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/command_line_interface.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_enum.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_enum_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_extension.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_file.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_helpers.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_map_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_message.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_message_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_padding_optimizer.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_parse_function_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_primitive_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_service.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/cpp_string_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_doc_comment.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_enum.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_enum_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_field_base.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_helpers.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_map_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_message.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_message_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_primitive_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_reflection_class.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_repeated_enum_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_repeated_message_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_repeated_primitive_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_source_generator_base.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/csharp_wrapper_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_context.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_doc_comment.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_enum.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_enum_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_enum_field_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_enum_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_extension.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_extension_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_file.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_generator_factory.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_helpers.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_kotlin_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_map_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_map_field_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_message.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_message_builder.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_message_builder_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_message_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_message_field_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_message_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_name_resolver.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_primitive_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_primitive_field_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_service.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_shared_code_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_string_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/java_string_field_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/js/js_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/js/well_known_types_embed.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/objectivec_enum.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/objectivec_enum_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/objectivec_extension.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/objectivec_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/objectivec_file.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/objectivec_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/objectivec_helpers.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/objectivec_map_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/objectivec_message.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/objectivec_message_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/objectivec_oneof.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/objectivec_primitive_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/php/php_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/plugin.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/plugin.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/python/python_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/ruby/ruby_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/subprocess.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/zip_writer.cc
)

add_library(_libprotoc ${libprotoc_files})
target_link_libraries(_libprotoc _libprotobuf)
add_library(protobuf::libprotoc ALIAS _libprotoc)

set(protoc_files ${protobuf_source_dir}/src/google/protobuf/compiler/main.cc)

if (CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME
    AND CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR)

    add_executable(protoc ${protoc_files})
    target_link_libraries(protoc _libprotoc _libprotobuf pthread)
    add_executable(protobuf::protoc ALIAS protoc)

    if (ENABLE_FUZZING)
        # `protoc` will be built with sanitizer and it could fail during ClickHouse build
        # It easily reproduces in oss-fuzz building pipeline
        # To avoid this we can try to build `protoc` without any sanitizer with option `-fno-sanitize=all`, but
        # it this case we will face with linker errors, because libcxx still will be built with sanitizer
        # So, we can simply suppress all of these failures with a combination this flag and an environment variable
        # export MSAN_OPTIONS=exit_code=0
        target_compile_options(protoc PRIVATE "-fsanitize-recover=all")
    endif()
else ()
    # Build 'protoc' for host arch
    set (PROTOC_BUILD_DIR "${protobuf_binary_dir}/build")

    if (NOT EXISTS "${PROTOC_BUILD_DIR}/protoc")

        # This is quite ugly but I cannot make dependencies work propery.

        execute_process(
            COMMAND mkdir -p ${PROTOC_BUILD_DIR}
            COMMAND_ECHO STDOUT)

        execute_process(
            COMMAND ${CMAKE_COMMAND}
                "-G${CMAKE_GENERATOR}"
                "-DCMAKE_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}"
                "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
                "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
                "-Dprotobuf_BUILD_TESTS=0"
                "-Dprotobuf_BUILD_CONFORMANCE=0"
                "-Dprotobuf_BUILD_EXAMPLES=0"
                "-Dprotobuf_BUILD_PROTOC_BINARIES=1"
                "${protobuf_source_dir}/cmake"
            WORKING_DIRECTORY "${PROTOC_BUILD_DIR}"
            COMMAND_ECHO STDOUT)

        execute_process(
            COMMAND ${CMAKE_COMMAND} --build "${PROTOC_BUILD_DIR}"
            COMMAND_ECHO STDOUT)
    endif ()

#    add_custom_command (
#        OUTPUT ${PROTOC_BUILD_DIR}
#        COMMAND mkdir -p ${PROTOC_BUILD_DIR})
#
#    add_custom_command (
#        OUTPUT "${PROTOC_BUILD_DIR}/CMakeCache.txt"
#
#        COMMAND ${CMAKE_COMMAND}
#             -G"${CMAKE_GENERATOR}"
#             -DCMAKE_MAKE_PROGRAM="${CMAKE_MAKE_PROGRAM}"
#             -DCMAKE_C_COMPILER="${CMAKE_C_COMPILER}"
#             -DCMAKE_CXX_COMPILER="${CMAKE_CXX_COMPILER}"
#             -Dprotobuf_BUILD_TESTS=0
#             -Dprotobuf_BUILD_CONFORMANCE=0
#             -Dprotobuf_BUILD_EXAMPLES=0
#             -Dprotobuf_BUILD_PROTOC_BINARIES=1
#             "${protobuf_source_dir}/cmake"
#
#         DEPENDS "${PROTOC_BUILD_DIR}"
#         WORKING_DIRECTORY "${PROTOC_BUILD_DIR}"
#         COMMENT "Configuring 'protoc' for host architecture."
#         USES_TERMINAL)
#
#     add_custom_command (
#         OUTPUT "${PROTOC_BUILD_DIR}/protoc"
#         COMMAND ${CMAKE_COMMAND} --build "${PROTOC_BUILD_DIR}"
#         DEPENDS "${PROTOC_BUILD_DIR}/CMakeCache.txt"
#         COMMENT "Building 'protoc' for host architecture."
#         USES_TERMINAL)
#
#     add_custom_target (protoc-host DEPENDS "${PROTOC_BUILD_DIR}/protoc")

    add_executable(protoc IMPORTED GLOBAL)
    set_target_properties (protoc PROPERTIES IMPORTED_LOCATION "${PROTOC_BUILD_DIR}/protoc")
    add_dependencies(protoc "${PROTOC_BUILD_DIR}/protoc")
endif ()

include("${ClickHouse_SOURCE_DIR}/contrib/protobuf-cmake/protobuf_generate.cmake")

add_library(_protobuf INTERFACE)
target_link_libraries(_protobuf INTERFACE _libprotobuf)
target_include_directories(_protobuf INTERFACE "${Protobuf_INCLUDE_DIR}")
add_library(ch_contrib::protobuf ALIAS _protobuf)

add_library(_protoc INTERFACE)
target_link_libraries(_protoc INTERFACE _libprotoc _libprotobuf)
target_include_directories(_protoc INTERFACE "${Protobuf_INCLUDE_DIR}")
add_library(ch_contrib::protoc ALIAS _protoc)
