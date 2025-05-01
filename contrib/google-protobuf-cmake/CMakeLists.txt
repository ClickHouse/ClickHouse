option(ENABLE_PROTOBUF "Enable protobuf" ${ENABLE_LIBRARIES})

if(NOT ENABLE_PROTOBUF)
  message(STATUS "Not using protobuf")
  return()
endif()

# To avoid errors "'X' does not refer to a value" while using `offsetof` function.
set(CMAKE_CXX_STANDARD 17)

set(Protobuf_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src")
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

set(protobuf_source_dir "${ClickHouse_SOURCE_DIR}/contrib/google-protobuf")
set(protobuf_binary_dir "${ClickHouse_BINARY_DIR}/contrib/google-protobuf")

add_definitions(-DGOOGLE_PROTOBUF_CMAKE_BUILD)

add_definitions(-DHAVE_PTHREAD)
add_definitions(-DHAVE_ZLIB)

include_directories(
  ${protobuf_binary_dir}
  ${protobuf_source_dir}/src)

add_library(utf8_range
    ${protobuf_source_dir}/third_party/utf8_range/naive.c
    ${protobuf_source_dir}/third_party/utf8_range/range2-neon.c
    ${protobuf_source_dir}/third_party/utf8_range/range2-sse.c
)
include_directories(${protobuf_source_dir}/third_party/utf8_range)

add_library(utf8_validity
    ${protobuf_source_dir}/third_party/utf8_range/utf8_validity.cc
)
target_link_libraries(utf8_validity PUBLIC absl::strings)

set(protobuf_absl_used_targets
    absl::absl_check
    absl::absl_log
    absl::algorithm
    absl::base
    absl::bind_front
    absl::bits
    absl::btree
    absl::cleanup
    absl::cord
    absl::core_headers
    absl::debugging
    absl::die_if_null
    absl::dynamic_annotations
    absl::flags
    absl::flat_hash_map
    absl::flat_hash_set
    absl::function_ref
    absl::hash
    absl::layout
    absl::log_initialize
    absl::log_severity
    absl::memory
    absl::node_hash_map
    absl::node_hash_set
    absl::optional
    absl::span
    absl::status
    absl::statusor
    absl::strings
    absl::synchronization
    absl::time
    absl::type_traits
    absl::utility
    absl::variant
)

set(libprotobuf_lite_files
  ${protobuf_source_dir}/src/google/protobuf/any_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/arena.cc
  ${protobuf_source_dir}/src/google/protobuf/arena_align.cc
  ${protobuf_source_dir}/src/google/protobuf/arenastring.cc
  ${protobuf_source_dir}/src/google/protobuf/arenaz_sampler.cc
  ${protobuf_source_dir}/src/google/protobuf/extension_set.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_enum_util.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_message_tctable_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_message_util.cc
  ${protobuf_source_dir}/src/google/protobuf/implicit_weak_message.cc
  ${protobuf_source_dir}/src/google/protobuf/inlined_string_field.cc
  ${protobuf_source_dir}/src/google/protobuf/io/coded_stream.cc
  ${protobuf_source_dir}/src/google/protobuf/io/io_win32.cc
  ${protobuf_source_dir}/src/google/protobuf/io/zero_copy_stream.cc
  ${protobuf_source_dir}/src/google/protobuf/io/zero_copy_stream_impl.cc
  ${protobuf_source_dir}/src/google/protobuf/io/zero_copy_stream_impl_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/map.cc
  ${protobuf_source_dir}/src/google/protobuf/message_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/parse_context.cc
  ${protobuf_source_dir}/src/google/protobuf/repeated_field.cc
  ${protobuf_source_dir}/src/google/protobuf/repeated_ptr_field.cc
  ${protobuf_source_dir}/src/google/protobuf/stubs/common.cc
  ${protobuf_source_dir}/src/google/protobuf/wire_format_lite.cc
)

add_library(_libprotobuf-lite ${libprotobuf_lite_files})
target_link_libraries(_libprotobuf-lite
    pthread
    utf8_validity)
if(${CMAKE_SYSTEM_NAME} STREQUAL "Android")
    target_link_libraries(_libprotobuf-lite log)
endif()
target_include_directories(_libprotobuf-lite SYSTEM PUBLIC ${protobuf_source_dir}/src)
add_library(protobuf::libprotobuf-lite ALIAS _libprotobuf-lite)


set(libprotobuf_files
  ${protobuf_source_dir}/src/google/protobuf/any.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/api.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/duration.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/empty.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/field_mask.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/source_context.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/struct.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/timestamp.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/type.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/wrappers.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/any.cc
  ${protobuf_source_dir}/src/google/protobuf/any_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/arena.cc
  ${protobuf_source_dir}/src/google/protobuf/arena_align.cc
  ${protobuf_source_dir}/src/google/protobuf/arenastring.cc
  ${protobuf_source_dir}/src/google/protobuf/arenaz_sampler.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/importer.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/parser.cc
  ${protobuf_source_dir}/src/google/protobuf/cpp_features.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/descriptor.cc
  ${protobuf_source_dir}/src/google/protobuf/descriptor.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/descriptor_database.cc
  ${protobuf_source_dir}/src/google/protobuf/dynamic_message.cc
  ${protobuf_source_dir}/src/google/protobuf/extension_set.cc
  ${protobuf_source_dir}/src/google/protobuf/extension_set_heavy.cc
  ${protobuf_source_dir}/src/google/protobuf/feature_resolver.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_enum_util.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_message_bases.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_message_reflection.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_message_tctable_full.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_message_tctable_gen.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_message_tctable_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/generated_message_util.cc
  ${protobuf_source_dir}/src/google/protobuf/implicit_weak_message.cc
  ${protobuf_source_dir}/src/google/protobuf/inlined_string_field.cc
  ${protobuf_source_dir}/src/google/protobuf/io/coded_stream.cc
  ${protobuf_source_dir}/src/google/protobuf/io/gzip_stream.cc
  ${protobuf_source_dir}/src/google/protobuf/io/io_win32.cc
  ${protobuf_source_dir}/src/google/protobuf/io/printer.cc
  ${protobuf_source_dir}/src/google/protobuf/io/strtod.cc
  ${protobuf_source_dir}/src/google/protobuf/io/tokenizer.cc
  ${protobuf_source_dir}/src/google/protobuf/io/zero_copy_sink.cc
  ${protobuf_source_dir}/src/google/protobuf/io/zero_copy_stream.cc
  ${protobuf_source_dir}/src/google/protobuf/io/zero_copy_stream_impl.cc
  ${protobuf_source_dir}/src/google/protobuf/io/zero_copy_stream_impl_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/json/internal/lexer.cc
  ${protobuf_source_dir}/src/google/protobuf/json/internal/message_path.cc
  ${protobuf_source_dir}/src/google/protobuf/json/internal/parser.cc
  ${protobuf_source_dir}/src/google/protobuf/json/internal/unparser.cc
  ${protobuf_source_dir}/src/google/protobuf/json/internal/untyped_message.cc
  ${protobuf_source_dir}/src/google/protobuf/json/internal/writer.cc
  ${protobuf_source_dir}/src/google/protobuf/json/internal/zero_copy_buffered_stream.cc
  ${protobuf_source_dir}/src/google/protobuf/json/json.cc
  ${protobuf_source_dir}/src/google/protobuf/map.cc
  ${protobuf_source_dir}/src/google/protobuf/map_field.cc
  ${protobuf_source_dir}/src/google/protobuf/message.cc
  ${protobuf_source_dir}/src/google/protobuf/message_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/parse_context.cc
  ${protobuf_source_dir}/src/google/protobuf/port.cc
  ${protobuf_source_dir}/src/google/protobuf/raw_ptr.cc
  ${protobuf_source_dir}/src/google/protobuf/reflection_mode.cc
  ${protobuf_source_dir}/src/google/protobuf/reflection_ops.cc
  ${protobuf_source_dir}/src/google/protobuf/repeated_field.cc
  ${protobuf_source_dir}/src/google/protobuf/repeated_ptr_field.cc
  ${protobuf_source_dir}/src/google/protobuf/service.cc
  ${protobuf_source_dir}/src/google/protobuf/stubs/common.cc
  ${protobuf_source_dir}/src/google/protobuf/text_format.cc
  ${protobuf_source_dir}/src/google/protobuf/unknown_field_set.cc
  ${protobuf_source_dir}/src/google/protobuf/util/delimited_message_util.cc
  ${protobuf_source_dir}/src/google/protobuf/util/field_comparator.cc
  ${protobuf_source_dir}/src/google/protobuf/util/field_mask_util.cc
  ${protobuf_source_dir}/src/google/protobuf/util/message_differencer.cc
  ${protobuf_source_dir}/src/google/protobuf/util/time_util.cc
  ${protobuf_source_dir}/src/google/protobuf/util/type_resolver_util.cc
  ${protobuf_source_dir}/src/google/protobuf/wire_format.cc
  ${protobuf_source_dir}/src/google/protobuf/wire_format_lite.cc
)

add_library(_libprotobuf ${libprotobuf_lite_files} ${libprotobuf_files})
if (ENABLE_FUZZING)
    target_compile_options(_libprotobuf PRIVATE "-fsanitize-recover=all")
endif()
target_link_libraries(_libprotobuf
    pthread
    ch_contrib::zlib
    utf8_validity
    ${protobuf_absl_used_targets})
if(${CMAKE_SYSTEM_NAME} STREQUAL "Android")
    target_link_libraries(_libprotobuf log)
endif()
target_include_directories(_libprotobuf SYSTEM PUBLIC ${protobuf_source_dir}/src)
add_library(protobuf::libprotobuf ALIAS _libprotobuf)


set(libprotoc_files
  ${protobuf_source_dir}/src/google/protobuf/compiler/allowlists/editions.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/code_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/command_line_interface.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/enum.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/extension.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/field_generators/cord_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/field_generators/enum_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/field_generators/map_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/field_generators/message_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/field_generators/primitive_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/field_generators/string_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/file.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/helpers.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/message.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/padding_optimizer.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/parse_function_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/service.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/cpp/tracker.cc
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
  ${protobuf_source_dir}/src/google/protobuf/compiler/csharp/names.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/context.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/doc_comment.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/enum.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/enum_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/enum_field_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/enum_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/extension.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/extension_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/file.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/generator_factory.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/helpers.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/kotlin_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/map_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/map_field_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/message.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/message_builder.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/message_builder_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/message_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/message_field_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/message_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/message_serialization.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/name_resolver.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/names.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/primitive_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/primitive_field_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/service.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/shared_code_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/string_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/java/string_field_lite.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/enum.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/enum_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/extension.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/file.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/helpers.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/import_writer.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/line_consumer.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/map_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/message.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/message_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/names.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/oneof.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/primitive_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/objectivec/text_format_decode_data.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/php/names.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/php/php_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/plugin.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/plugin.pb.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/python/generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/python/helpers.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/python/pyi_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/retention.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/ruby/ruby_generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/rust/accessors/accessors.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/rust/accessors/singular_message.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/rust/accessors/singular_scalar.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/rust/accessors/singular_string.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/rust/accessors/unsupported_field.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/rust/context.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/rust/generator.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/rust/message.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/rust/naming.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/rust/oneof.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/rust/relative_path.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/subprocess.cc
  ${protobuf_source_dir}/src/google/protobuf/compiler/zip_writer.cc
)

add_library(_libprotoc ${libprotoc_files})
target_link_libraries(_libprotoc
    _libprotobuf
    ${protobuf_absl_used_targets})
add_library(protobuf::libprotoc ALIAS _libprotoc)

set(protoc_files ${protobuf_source_dir}/src/google/protobuf/compiler/main.cc)

if (CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME
    AND CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR)

    add_executable(protoc ${protoc_files})
    target_link_libraries(protoc _libprotoc
        _libprotobuf
        pthread
        utf8_validity
        ${protobuf_absl_used_targets})
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

        set(abseil_source_dir "${ClickHouse_SOURCE_DIR}/contrib/abseil-cpp")

        execute_process(
            COMMAND mkdir -p ${PROTOC_BUILD_DIR}
            COMMAND_ECHO STDOUT
            COMMAND_ERROR_IS_FATAL ANY
        )

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
                "-DABSL_ROOT_DIR=${abseil_source_dir}"
                "-DABSL_ENABLE_INSTALL=0"
                "${protobuf_source_dir}"
            WORKING_DIRECTORY "${PROTOC_BUILD_DIR}"
            COMMAND_ECHO STDOUT
            COMMAND_ERROR_IS_FATAL ANY
        )

        execute_process(
            COMMAND ${CMAKE_COMMAND} --build "${PROTOC_BUILD_DIR}"
            COMMAND_ECHO STDOUT
            COMMAND_ERROR_IS_FATAL ANY
        )
    endif ()

    add_executable(protoc IMPORTED GLOBAL)
    set_target_properties (protoc PROPERTIES IMPORTED_LOCATION "${PROTOC_BUILD_DIR}/protoc")
    add_dependencies(protoc "${PROTOC_BUILD_DIR}/protoc")
endif ()

include("${ClickHouse_SOURCE_DIR}/contrib/google-protobuf-cmake/protobuf_generate.cmake")

# These files needs to be installed to make it possible that users can use well-known protobuf types
set(google_proto_files
  ${protobuf_source_dir}/src/google/protobuf/any.proto
  ${protobuf_source_dir}/src/google/protobuf/api.proto
  ${protobuf_source_dir}/src/google/protobuf/descriptor.proto
  ${protobuf_source_dir}/src/google/protobuf/duration.proto
  ${protobuf_source_dir}/src/google/protobuf/empty.proto
  ${protobuf_source_dir}/src/google/protobuf/field_mask.proto
  ${protobuf_source_dir}/src/google/protobuf/source_context.proto
  ${protobuf_source_dir}/src/google/protobuf/struct.proto
  ${protobuf_source_dir}/src/google/protobuf/timestamp.proto
  ${protobuf_source_dir}/src/google/protobuf/type.proto
  ${protobuf_source_dir}/src/google/protobuf/wrappers.proto
)

add_library(_protobuf INTERFACE)
target_link_libraries(_protobuf INTERFACE _libprotobuf)
target_include_directories(_protobuf INTERFACE "${Protobuf_INCLUDE_DIR}")
set_target_properties(_protobuf PROPERTIES google_proto_files "${google_proto_files}")
add_library(ch_contrib::protobuf ALIAS _protobuf)

add_library(_protoc INTERFACE)
target_link_libraries(_protoc INTERFACE _libprotoc _libprotobuf)
target_include_directories(_protoc INTERFACE "${Protobuf_INCLUDE_DIR}")
add_library(ch_contrib::protoc ALIAS _protoc)
