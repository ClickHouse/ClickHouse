# Arrow and Parquet are implemented in ClickHouse: the native Parquet reader and writer
# (`src/Processors/Formats/Impl/Parquet`), the native `Arrow` and `ArrowStream` formats and the Arrow C data interface
# (`src/Processors/Formats/Impl/ArrowIPC`), and Arrow Flight on gRPC (`src/Server/ArrowFlight`,
# `src/Storages/ArrowFlight`). The Apache Arrow C++ library and parquet-cpp are not built.
#
# Only the definitions of the formats are taken from `contrib/arrow`:
# - the FlatBuffers schemas of the Arrow IPC metadata (`format/*.fbs`);
# - the structures generated from `parquet.thrift` (`cpp/src/generated/parquet_types.*`);
# - the protocol of Arrow Flight (`format/Flight.proto` and `format/FlightSql.proto`).

# `ENABLE_PARQUET` is declared in `contrib/CMakeLists.txt`, before `flatbuffers-cmake`.
if (NOT ENABLE_PARQUET)
    message(STATUS "Not using parquet")
    return()
endif()

set(ARROW_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src")

# === Arrow IPC metadata bindings
# Generated with our `flatc` from the schemas of the format. `Message.fbs` includes the schemas of tensors,
# so their bindings are generated too.

if (NOT CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME OR NOT CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR)
    set(FLATC_EXECUTABLE "${PROJECT_BINARY_DIR}/native/contrib/flatbuffers-cmake/flatc")
else ()
    set(FLATC_EXECUTABLE $<TARGET_FILE:flatc>)
endif()

set(ARROW_FORMAT_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/format")
# The bindings live in a "generated" subdirectory of the include directory: #include <generated/Message_generated.h>.
set(ARROW_IPC_GENERATED_INCLUDE_DIR "${ClickHouse_BINARY_DIR}/contrib/flatbuffers")
set(ARROW_IPC_GENERATED_DIR "${ARROW_IPC_GENERATED_INCLUDE_DIR}/generated")

set(ARROW_FBS
  ${ARROW_FORMAT_DIR}/File.fbs
  ${ARROW_FORMAT_DIR}/Message.fbs
  ${ARROW_FORMAT_DIR}/Schema.fbs
  ${ARROW_FORMAT_DIR}/SparseTensor.fbs
  ${ARROW_FORMAT_DIR}/Tensor.fbs)

set(ARROW_IPC_GENERATED_HEADERS
  ${ARROW_IPC_GENERATED_DIR}/File_generated.h
  ${ARROW_IPC_GENERATED_DIR}/Message_generated.h
  ${ARROW_IPC_GENERATED_DIR}/Schema_generated.h
  ${ARROW_IPC_GENERATED_DIR}/SparseTensor_generated.h
  ${ARROW_IPC_GENERATED_DIR}/Tensor_generated.h)

add_custom_command(OUTPUT ${ARROW_IPC_GENERATED_HEADERS}
        COMMAND ${CMAKE_COMMAND} -E make_directory "${ARROW_IPC_GENERATED_DIR}"
        COMMAND ${FLATC_EXECUTABLE}
        --cpp
        --cpp-std c++11
        --no-warnings
        -o "${ARROW_IPC_GENERATED_DIR}"
        ${ARROW_FBS}
        DEPENDS ${ARROW_FBS} ${FLATC_EXECUTABLE})

add_custom_target(_arrow_ipc_generated DEPENDS ${ARROW_IPC_GENERATED_HEADERS})

# Interface target exposing only the FlatBuffers-generated Arrow IPC metadata headers
# (Message_generated.h, Schema_generated.h, File_generated.h) plus the FlatBuffers library.
# The native Arrow IPC reader/writer (src/Processors/Formats/Impl/ArrowIPC) parses/builds the IPC
# metadata with FlatBuffers and decodes the buffers directly to/from ClickHouse columns.
# The sources of Arrow are not on the include path: no code of ClickHouse can include its headers.
add_library(_arrow_ipc_metadata INTERFACE)
target_include_directories(_arrow_ipc_metadata SYSTEM INTERFACE "${ARROW_IPC_GENERATED_INCLUDE_DIR}")
target_link_libraries(_arrow_ipc_metadata INTERFACE ch_contrib::flatbuffers)
add_dependencies(_arrow_ipc_metadata _arrow_ipc_generated)
add_library(ch_contrib::arrow_ipc_metadata ALIAS _arrow_ipc_metadata)

# === Parquet format
# The structures generated from `parquet.thrift` with the minimal Thrift runtime to (de)serialize them:
# everything the native Parquet reader and writer need from third-party code. The generated code is
# compiled from a copy in a directory of its own.
#
# The enums are given the fixed underlying type `int32_t`, the type of a Thrift enum on the wire. Without it, a value
# outside the range of an enum, which a corrupted file may contain, is undefined behavior as soon as it is loaded, and
# the generated code loads it before the reader can validate it: e.g. when a vector of `SchemaElement` grows and moves
# its elements. With a fixed underlying type, every `int32_t` is a valid value of the enum.
set(PARQUET_FORMAT_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/parquet_format_include")
foreach (file parquet_types.h parquet_types.tcc parquet_types.cpp)
    set(source "${ARROW_SRC_DIR}/generated/${file}")
    set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${source}")
    file(READ "${source}" content)
    string(REPLACE "  enum type {" "  enum type : int32_t {" content "${content}")
    set(output "${PARQUET_FORMAT_INCLUDE_DIR}/generated/${file}")
    set(previous_content "")
    if (EXISTS "${output}")
        file(READ "${output}" previous_content)
    endif()
    # Rewriting an unchanged file would rebuild everything that includes it after each run of CMake.
    if (NOT previous_content STREQUAL content)
        file(WRITE "${output}" "${content}")
    endif()
endforeach()
file(READ "${PARQUET_FORMAT_INCLUDE_DIR}/generated/parquet_types.h" content)
string(REGEX MATCHALL "enum type {" unfixed_enums "${content}")
if (unfixed_enums)
    message(FATAL_ERROR "Not all enums of parquet_types.h got a fixed underlying type")
endif()
# The only Arrow header included by the generated code. It does nothing outside Windows.
file(WRITE "${PARQUET_FORMAT_INCLUDE_DIR}/parquet/windows_compatibility.h" "#pragma once\n")

add_library(_parquet_format "${PARQUET_FORMAT_INCLUDE_DIR}/generated/parquet_types.cpp")
target_include_directories(_parquet_format SYSTEM PUBLIC "${PARQUET_FORMAT_INCLUDE_DIR}")
target_link_libraries(_parquet_format PUBLIC ch_contrib::thrift_minimal)
add_library(ch_contrib::parquet_format ALIAS _parquet_format)

# === Arrow Flight protocol

include(flight.cmake)
