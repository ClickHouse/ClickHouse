# FlatBuffers: the runtime library, used by the native Arrow IPC reader/writer and by Arrow itself,
# and the `flatc` schema compiler, which generates the Arrow IPC metadata bindings at build time
# (see `contrib/arrow-cmake`).
#
# Because `flatc` runs during the build, this directory is added from the window in
# `contrib/CMakeLists.txt` where the real compiler and linker are used even in clang-tidy builds,
# next to `protoc`. Elsewhere the dummy launchers would leave `flatc` as an empty file. The
# launchers are switched per directory (`CMAKE_AR` and `CMAKE_EXE_LINKER_FLAGS` are read when a
# directory is done), so the whole directory has to sit inside that window; wrapping just the
# targets does not work.

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/flatbuffers")

set(FLATBUFFERS_SRCS
  ${LIBRARY_DIR}/src/idl_parser.cpp
  ${LIBRARY_DIR}/src/file_manager.cpp
  ${LIBRARY_DIR}/src/file_name_manager.cpp
  ${LIBRARY_DIR}/src/idl_gen_text.cpp
  ${LIBRARY_DIR}/src/reflection.cpp
  ${LIBRARY_DIR}/src/util.cpp)

add_library(_flatbuffers STATIC ${FLATBUFFERS_SRCS})
# SYSTEM so the FlatBuffers headers don't trip ClickHouse's -Werror -Weverything (e.g. -Wdocumentation)
# in consumers that include them directly, such as the native Arrow IPC reader/writer.
target_include_directories(_flatbuffers SYSTEM PUBLIC "${LIBRARY_DIR}/include")
target_compile_definitions(_flatbuffers PRIVATE -DFLATBUFFERS_LOCALE_INDEPENDENT=0)
add_library(ch_contrib::flatbuffers ALIAS _flatbuffers)

# === flatc
#
# Generated FlatBuffers code contains a `static_assert` on `FLATBUFFERS_VERSION_MAJOR`/`MINOR`/
# `REVISION` that pins it to the exact FlatBuffers release it was produced with, so pre-generated
# bindings (such as the ones Arrow ships) cannot be compiled against any other version of the
# library. Build the compiler and regenerate the bindings from the schemas instead.

set(FLATC_SRCS
  ${LIBRARY_DIR}/src/annotated_binary_text_gen.cpp
  ${LIBRARY_DIR}/src/bfbs_gen_lua.cpp
  ${LIBRARY_DIR}/src/bfbs_gen_nim.cpp
  ${LIBRARY_DIR}/src/binary_annotator.cpp
  ${LIBRARY_DIR}/src/code_generators.cpp
  ${LIBRARY_DIR}/src/flatc.cpp
  ${LIBRARY_DIR}/src/flatc_main.cpp
  ${LIBRARY_DIR}/src/idl_gen_binary.cpp
  ${LIBRARY_DIR}/src/idl_gen_cpp.cpp
  ${LIBRARY_DIR}/src/idl_gen_csharp.cpp
  ${LIBRARY_DIR}/src/idl_gen_dart.cpp
  ${LIBRARY_DIR}/src/idl_gen_fbs.cpp
  ${LIBRARY_DIR}/src/idl_gen_go.cpp
  ${LIBRARY_DIR}/src/idl_gen_grpc.cpp
  ${LIBRARY_DIR}/src/idl_gen_java.cpp
  ${LIBRARY_DIR}/src/idl_gen_json_schema.cpp
  ${LIBRARY_DIR}/src/idl_gen_kotlin.cpp
  ${LIBRARY_DIR}/src/idl_gen_kotlin_kmp.cpp
  ${LIBRARY_DIR}/src/idl_gen_lobster.cpp
  ${LIBRARY_DIR}/src/idl_gen_php.cpp
  ${LIBRARY_DIR}/src/idl_gen_python.cpp
  ${LIBRARY_DIR}/src/idl_gen_rust.cpp
  ${LIBRARY_DIR}/src/idl_gen_swift.cpp
  ${LIBRARY_DIR}/src/idl_gen_ts.cpp
  ${LIBRARY_DIR}/include/codegen/python.cc
  ${LIBRARY_DIR}/grpc/src/compiler/cpp_generator.cc
  ${LIBRARY_DIR}/grpc/src/compiler/go_generator.cc
  ${LIBRARY_DIR}/grpc/src/compiler/java_generator.cc
  ${LIBRARY_DIR}/grpc/src/compiler/python_generator.cc
  ${LIBRARY_DIR}/grpc/src/compiler/swift_generator.cc
  ${LIBRARY_DIR}/grpc/src/compiler/ts_generator.cc)

add_executable(flatc ${FLATC_SRCS})
target_link_libraries(flatc PRIVATE _flatbuffers)
target_include_directories(flatc SYSTEM PRIVATE
  "${LIBRARY_DIR}/include"
  "${LIBRARY_DIR}"
  "${LIBRARY_DIR}/grpc")
target_compile_definitions(flatc PRIVATE -DFLATBUFFERS_LOCALE_INDEPENDENT=0)

# When cross-compiling, `flatc` has to run on the host, so it is built by the native sub-build
# (see `add_native_target` in the top-level `CMakeLists.txt`), same as `protoc`.
add_native_target (flatc)
if (NOT CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME OR NOT CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR OR USE_MUSL)
    set_target_properties (flatc PROPERTIES IMPORTED_LOCATION "${PROJECT_BINARY_DIR}/native/contrib/flatbuffers-cmake/flatc")
endif ()
