# We dont use arrow's cmakefiles because they uses too many depends and download some libs in compile time
# But you can update auto-generated parquet files manually:
# cd {BUILD_DIR}/contrib/arrow/cpp/src/parquet && mkdir -p build && cd build
# cmake .. -DARROW_COMPUTE=ON -DARROW_PARQUET=ON -DARROW_SIMD_LEVEL=NONE -DARROW_VERBOSE_THIRDPARTY_BUILD=ON
#          -DARROW_BUILD_SHARED=1 -DARROW_BUILD_UTILITIES=OFF -DARROW_BUILD_INTEGRATION=OFF
#          -DBoost_FOUND=1 -DARROW_TEST_LINKAGE="shared"
# make -j8
# copy {BUILD_DIR}/contrib/arrow/cpp/src/parquet/*.cpp,*.h -> {BUILD_DIR}/contrib/arrow-cmake/cpp/src/parquet/

# Also useful parquet reader:
# cd {BUILD_DIR}/contrib/arrow/cpp && mkdir -p build && cd build
# cmake .. -DARROW_PARQUET=1 -DARROW_WITH_SNAPPY=1 -DPARQUET_BUILD_EXECUTABLES=1
# make -j8
# {BUILD_DIR}/contrib/arrow/cpp/build/release/parquet-reader some_file.parquet

set (ENABLE_PARQUET_DEFAULT ${ENABLE_LIBRARIES})
if (OS_FREEBSD)
    set (ENABLE_PARQUET_DEFAULT OFF)
endif()
option (ENABLE_PARQUET "Enable parquet" ${ENABLE_PARQUET_DEFAULT})

if (NOT ENABLE_PARQUET)
    message(STATUS "Not using parquet")
    return()
endif()

# Freebsd: ../contrib/arrow/cpp/src/arrow/util/bit-util.h:27:10: fatal error: endian.h: No such file or directory
if (OS_FREEBSD)
    message (FATAL_ERROR "Using internal parquet library on FreeBSD is not supported")
endif()

set (CMAKE_CXX_STANDARD 17)

set(ARROW_VERSION "11.0.0")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" ARROW_BASE_VERSION "${ARROW_VERSION}")

set(ARROW_VERSION_MAJOR "11")
set(ARROW_VERSION_MINOR "0")
set(ARROW_VERSION_PATCH "0")

if(ARROW_VERSION_MAJOR STREQUAL "0")
    # Arrow 0.x.y => SO version is "x", full SO version is "x.y.0"
    set(ARROW_SO_VERSION "${ARROW_VERSION_MINOR}")
    set(ARROW_FULL_SO_VERSION "${ARROW_SO_VERSION}.${ARROW_VERSION_PATCH}.0")
else()
    # Arrow 1.x.y => SO version is "10x", full SO version is "10x.y.0"
    math(EXPR ARROW_SO_VERSION "${ARROW_VERSION_MAJOR} * 100 + ${ARROW_VERSION_MINOR}")
    set(ARROW_FULL_SO_VERSION "${ARROW_SO_VERSION}.${ARROW_VERSION_PATCH}.0")
endif()


# === orc
set(ORC_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/orc/c++")
set(ORC_INCLUDE_DIR "${ORC_SOURCE_DIR}/include")
set(ORC_SOURCE_SRC_DIR "${ORC_SOURCE_DIR}/src")
# set(ORC_SOURCE_WRAP_DIR "${ORC_SOURCE_DIR}/wrap")

set(ORC_BUILD_SRC_DIR "${CMAKE_CURRENT_BINARY_DIR}/../orc/c++/src")
set(ORC_BUILD_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/../orc/c++/include")

set(ORC_ADDITION_SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(ARROW_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src")

set(PROTOBUF_EXECUTABLE $<TARGET_FILE:protoc>)
set(PROTO_DIR "${ORC_SOURCE_DIR}/../proto")


add_custom_command(OUTPUT orc_proto.pb.h orc_proto.pb.cc
        COMMAND ${PROTOBUF_EXECUTABLE}
        -I ${PROTO_DIR}
        --cpp_out="${CMAKE_CURRENT_BINARY_DIR}"
        "${PROTO_DIR}/orc_proto.proto")


# === flatbuffers
set(FLATBUFFERS_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/flatbuffers")
set(FLATBUFFERS_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/flatbuffers")
set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_SRC_DIR}/include")

set(FLATBUFFERS_SRCS
  ${FLATBUFFERS_SRC_DIR}/src/idl_parser.cpp
  ${FLATBUFFERS_SRC_DIR}/src/idl_gen_text.cpp
  ${FLATBUFFERS_SRC_DIR}/src/reflection.cpp
  ${FLATBUFFERS_SRC_DIR}/src/util.cpp)

add_library(_flatbuffers STATIC ${FLATBUFFERS_SRCS})
target_include_directories(_flatbuffers PUBLIC ${FLATBUFFERS_INCLUDE_DIR})
target_compile_definitions(_flatbuffers PRIVATE -DFLATBUFFERS_LOCALE_INDEPENDENT=0)


# ===  hdfs
# NOTE: cannot use ch_contrib::hdfs since it's INCLUDE_DIRECTORIES does not includes trailing "hdfs/"
set(HDFS_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libhdfs3/include/hdfs/")

# arrow-cmake cmake file calling orc cmake subroutine which detects certain compiler features.
# Apple Clang compiler failed to compile this code without specifying c++11 standard.
# As result these compiler features detected as absent. In result it failed to compile orc itself.
# In orc makefile there is code that sets flags, but arrow-cmake ignores these flags.
if (CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    set(CXX11_FLAGS "-std=c++0x")
endif ()

set (ORC_CXX_HAS_INITIALIZER_LIST 1)
set (ORC_CXX_HAS_NOEXCEPT 1)
set (ORC_CXX_HAS_NULLPTR 1)
set (ORC_CXX_HAS_OVERRIDE 1)
set (ORC_CXX_HAS_UNIQUE_PTR 1)
set (ORC_CXX_HAS_CSTDINT 1)
set (ORC_CXX_HAS_THREAD_LOCAL 1)

include(orc_check.cmake)
configure_file("${ORC_SOURCE_SRC_DIR}/Adaptor.hh.in" "${ORC_BUILD_INCLUDE_DIR}/Adaptor.hh")


# ARROW_ORC + adapters/orc/CMakefiles
set(ORC_SRCS
        "${CMAKE_CURRENT_BINARY_DIR}/orc_proto.pb.h"
        "${ORC_ADDITION_SOURCE_DIR}/orc_proto.pb.cc"
        "${ORC_SOURCE_SRC_DIR}/Adaptor.cc"
        "${ORC_SOURCE_SRC_DIR}/Adaptor.hh.in"
        "${ORC_SOURCE_SRC_DIR}/BlockBuffer.cc"
        "${ORC_SOURCE_SRC_DIR}/BlockBuffer.hh"
        "${ORC_SOURCE_SRC_DIR}/BloomFilter.cc"
        "${ORC_SOURCE_SRC_DIR}/BloomFilter.hh"
        "${ORC_SOURCE_SRC_DIR}/Bpacking.hh"
        "${ORC_SOURCE_SRC_DIR}/BpackingDefault.cc"
        "${ORC_SOURCE_SRC_DIR}/BpackingDefault.hh"
        "${ORC_SOURCE_SRC_DIR}/ByteRLE.cc"
        "${ORC_SOURCE_SRC_DIR}/ByteRLE.hh"
        "${ORC_SOURCE_SRC_DIR}/ColumnPrinter.cc"
        "${ORC_SOURCE_SRC_DIR}/ColumnReader.cc"
        "${ORC_SOURCE_SRC_DIR}/ColumnReader.hh"
        "${ORC_SOURCE_SRC_DIR}/ColumnWriter.cc"
        "${ORC_SOURCE_SRC_DIR}/ColumnWriter.hh"
        "${ORC_SOURCE_SRC_DIR}/Common.cc"
        "${ORC_SOURCE_SRC_DIR}/Compression.cc"
        "${ORC_SOURCE_SRC_DIR}/Compression.hh"
        "${ORC_SOURCE_SRC_DIR}/ConvertColumnReader.cc"
        "${ORC_SOURCE_SRC_DIR}/ConvertColumnReader.hh"
        "${ORC_SOURCE_SRC_DIR}/CpuInfoUtil.cc"
        "${ORC_SOURCE_SRC_DIR}/CpuInfoUtil.hh"
        "${ORC_SOURCE_SRC_DIR}/Dispatch.hh"
        "${ORC_SOURCE_SRC_DIR}/Exceptions.cc"
        "${ORC_SOURCE_SRC_DIR}/Int128.cc"
        "${ORC_SOURCE_SRC_DIR}/LzoDecompressor.cc"
        "${ORC_SOURCE_SRC_DIR}/LzoDecompressor.hh"
        "${ORC_SOURCE_SRC_DIR}/MemoryPool.cc"
        "${ORC_SOURCE_SRC_DIR}/Murmur3.cc"
        "${ORC_SOURCE_SRC_DIR}/Murmur3.hh"
        "${ORC_SOURCE_SRC_DIR}/Options.hh"
        "${ORC_SOURCE_SRC_DIR}/OrcFile.cc"
        "${ORC_SOURCE_SRC_DIR}/RLE.cc"
        "${ORC_SOURCE_SRC_DIR}/RLE.hh"
        "${ORC_SOURCE_SRC_DIR}/RLEV2Util.cc"
        "${ORC_SOURCE_SRC_DIR}/RLEV2Util.hh"
        "${ORC_SOURCE_SRC_DIR}/RLEv1.cc"
        "${ORC_SOURCE_SRC_DIR}/RLEv1.hh"
        "${ORC_SOURCE_SRC_DIR}/RLEv2.hh"
        "${ORC_SOURCE_SRC_DIR}/Reader.cc"
        "${ORC_SOURCE_SRC_DIR}/Reader.hh"
        "${ORC_SOURCE_SRC_DIR}/RleDecoderV2.cc"
        "${ORC_SOURCE_SRC_DIR}/RleEncoderV2.cc"
        "${ORC_SOURCE_SRC_DIR}/SchemaEvolution.cc"
        "${ORC_SOURCE_SRC_DIR}/SchemaEvolution.hh"
        "${ORC_SOURCE_SRC_DIR}/Statistics.cc"
        "${ORC_SOURCE_SRC_DIR}/Statistics.hh"
        "${ORC_SOURCE_SRC_DIR}/StripeStream.cc"
        "${ORC_SOURCE_SRC_DIR}/StripeStream.hh"
        "${ORC_SOURCE_SRC_DIR}/Timezone.cc"
        "${ORC_SOURCE_SRC_DIR}/Timezone.hh"
        "${ORC_SOURCE_SRC_DIR}/TypeImpl.cc"
        "${ORC_SOURCE_SRC_DIR}/TypeImpl.hh"
        "${ORC_SOURCE_SRC_DIR}/Utils.hh"
        "${ORC_SOURCE_SRC_DIR}/Vector.cc"
        "${ORC_SOURCE_SRC_DIR}/Writer.cc"
        "${ORC_SOURCE_SRC_DIR}/io/InputStream.cc"
        "${ORC_SOURCE_SRC_DIR}/io/InputStream.hh"
        "${ORC_SOURCE_SRC_DIR}/io/OutputStream.cc"
        "${ORC_SOURCE_SRC_DIR}/io/OutputStream.hh"
        "${ORC_SOURCE_SRC_DIR}/io/Cache.cc"
        "${ORC_SOURCE_SRC_DIR}/io/Cache.hh"
        "${ORC_SOURCE_SRC_DIR}/sargs/ExpressionTree.cc"
        "${ORC_SOURCE_SRC_DIR}/sargs/ExpressionTree.hh"
        "${ORC_SOURCE_SRC_DIR}/sargs/Literal.cc"
        "${ORC_SOURCE_SRC_DIR}/sargs/PredicateLeaf.cc"
        "${ORC_SOURCE_SRC_DIR}/sargs/PredicateLeaf.hh"
        "${ORC_SOURCE_SRC_DIR}/sargs/SargsApplier.cc"
        "${ORC_SOURCE_SRC_DIR}/sargs/SargsApplier.hh"
        "${ORC_SOURCE_SRC_DIR}/sargs/SearchArgument.cc"
        "${ORC_SOURCE_SRC_DIR}/sargs/SearchArgument.hh"
        "${ORC_SOURCE_SRC_DIR}/sargs/TruthValue.cc"
        )

add_library(_orc ${ORC_SRCS})
target_link_libraries(_orc PRIVATE
    ch_contrib::protobuf
    ch_contrib::lz4
    ch_contrib::snappy
    ch_contrib::zlib
    ch_contrib::zstd)
target_include_directories(_orc SYSTEM BEFORE PUBLIC
    ${ORC_INCLUDE_DIR}
    "${ClickHouse_SOURCE_DIR}/contrib/arrow-cmake/cpp/src/orc/c++/include")
target_compile_options(_orc PRIVATE
    -include "${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src/google/protobuf/stubs/port.h"
)
target_include_directories(_orc SYSTEM BEFORE PUBLIC ${ORC_BUILD_INCLUDE_DIR})
target_include_directories(_orc SYSTEM PRIVATE
    ${ORC_SOURCE_SRC_DIR}
    ${ORC_SOURCE_WRAP_DIR}
    ${ORC_BUILD_SRC_DIR}
    ${ORC_ADDITION_SOURCE_DIR}
    ${ARROW_SRC_DIR})


# === arrow

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/arrow")

# arrow/cpp/src/arrow/CMakeLists.txt (ARROW_SRCS + ARROW_COMPUTE + ARROW_IPC)
# find . \( -iname \*.cc -o -iname \*.cpp -o -iname \*.c \) | sort | awk '{print "\"${LIBRARY_DIR}" substr($1,2) "\"" }' | grep -v 'test.cc' | grep -v 'json' | grep -v 'flight' \|
# grep -v 'csv' | grep -v 'acero' | grep -v 'dataset' | grep -v 'testing' | grep -v 'gpu' | grep -v 'engine' | grep -v 'filesystem' | grep -v 'benchmark.cc'
set(ARROW_SRCS
        "${LIBRARY_DIR}/adapters/orc/adapter.cc"
        "${LIBRARY_DIR}/adapters/orc/options.cc"
        "${LIBRARY_DIR}/adapters/orc/util.cc"
        "${LIBRARY_DIR}/array/array_base.cc"
        "${LIBRARY_DIR}/array/array_binary.cc"
        "${LIBRARY_DIR}/array/array_decimal.cc"
        "${LIBRARY_DIR}/array/array_dict.cc"
        "${LIBRARY_DIR}/array/array_nested.cc"
        "${LIBRARY_DIR}/array/array_primitive.cc"
        "${LIBRARY_DIR}/array/array_run_end.cc"
        "${LIBRARY_DIR}/array/builder_adaptive.cc"
        "${LIBRARY_DIR}/array/builder_base.cc"
        "${LIBRARY_DIR}/array/builder_binary.cc"
        "${LIBRARY_DIR}/array/builder_decimal.cc"
        "${LIBRARY_DIR}/array/builder_dict.cc"
        "${LIBRARY_DIR}/array/builder_nested.cc"
        "${LIBRARY_DIR}/array/builder_primitive.cc"
        "${LIBRARY_DIR}/array/builder_run_end.cc"
        "${LIBRARY_DIR}/array/builder_union.cc"
        "${LIBRARY_DIR}/array/concatenate.cc"
        "${LIBRARY_DIR}/array/data.cc"
        "${LIBRARY_DIR}/array/diff.cc"
        "${LIBRARY_DIR}/array/util.cc"
        "${LIBRARY_DIR}/array/validate.cc"
        "${LIBRARY_DIR}/buffer.cc"
        "${LIBRARY_DIR}/builder.cc"
        "${LIBRARY_DIR}/c/bridge.cc"
        "${LIBRARY_DIR}/c/dlpack.cc"
        "${LIBRARY_DIR}/chunk_resolver.cc"
        "${LIBRARY_DIR}/chunked_array.cc"
        "${LIBRARY_DIR}/compare.cc"
        "${LIBRARY_DIR}/compute/api_aggregate.cc"
        "${LIBRARY_DIR}/compute/api_scalar.cc"
        "${LIBRARY_DIR}/compute/api_vector.cc"
        "${LIBRARY_DIR}/compute/cast.cc"
        "${LIBRARY_DIR}/compute/exec.cc"
        "${LIBRARY_DIR}/compute/expression.cc"
        "${LIBRARY_DIR}/compute/function.cc"
        "${LIBRARY_DIR}/compute/function_internal.cc"
        "${LIBRARY_DIR}/compute/kernel.cc"
        "${LIBRARY_DIR}/compute/kernels/aggregate_basic.cc"
        "${LIBRARY_DIR}/compute/kernels/aggregate_mode.cc"
        "${LIBRARY_DIR}/compute/kernels/aggregate_quantile.cc"
        "${LIBRARY_DIR}/compute/kernels/aggregate_tdigest.cc"
        "${LIBRARY_DIR}/compute/kernels/aggregate_var_std.cc"
        "${LIBRARY_DIR}/compute/kernels/codegen_internal.cc"
        "${LIBRARY_DIR}/compute/kernels/hash_aggregate.cc"
        "${LIBRARY_DIR}/compute/kernels/ree_util_internal.cc"
        "${LIBRARY_DIR}/compute/kernels/row_encoder.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_arithmetic.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_boolean.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_boolean.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_dictionary.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_extension.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_internal.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_nested.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_numeric.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_string.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_temporal.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_compare.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_if_else.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_nested.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_random.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_round.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_set_lookup.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_string_ascii.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_string_utf8.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_temporal_binary.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_temporal_unary.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_validity.cc"
        "${LIBRARY_DIR}/compute/kernels/util_internal.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_array_sort.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_cumulative_ops.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_hash.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_nested.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_pairwise.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_rank.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_replace.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_run_end_encode.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_select_k.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_selection.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_selection_filter_internal.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_selection_internal.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_selection_take_internal.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_sort.cc"
        "${LIBRARY_DIR}/compute/key_hash_internal.cc"
        "${LIBRARY_DIR}/compute/key_map_internal.cc"
        "${LIBRARY_DIR}/compute/light_array_internal.cc"
        "${LIBRARY_DIR}/compute/ordering.cc"
        "${LIBRARY_DIR}/compute/registry.cc"
        "${LIBRARY_DIR}/compute/row/compare_internal.cc"
        "${LIBRARY_DIR}/compute/row/encode_internal.cc"
        "${LIBRARY_DIR}/compute/row/grouper.cc"
        "${LIBRARY_DIR}/compute/row/row_internal.cc"
        "${LIBRARY_DIR}/compute/util.cc"
        "${LIBRARY_DIR}/config.cc"
        "${LIBRARY_DIR}/datum.cc"
        "${LIBRARY_DIR}/device.cc"
        "${LIBRARY_DIR}/extension_type.cc"
        "${LIBRARY_DIR}/integration/c_data_integration_internal.cc"
        "${LIBRARY_DIR}/io/buffered.cc"
        "${LIBRARY_DIR}/io/caching.cc"
        "${LIBRARY_DIR}/io/compressed.cc"
        "${LIBRARY_DIR}/io/file.cc"
        "${LIBRARY_DIR}/io/hdfs.cc"
        "${LIBRARY_DIR}/io/hdfs_internal.cc"
        "${LIBRARY_DIR}/io/interfaces.cc"
        "${LIBRARY_DIR}/io/memory.cc"
        "${LIBRARY_DIR}/io/slow.cc"
        "${LIBRARY_DIR}/io/stdio.cc"
        "${LIBRARY_DIR}/io/transform.cc"
        "${LIBRARY_DIR}/ipc/dictionary.cc"
        "${LIBRARY_DIR}/ipc/feather.cc"
        "${LIBRARY_DIR}/ipc/file_to_stream.cc"
        "${LIBRARY_DIR}/ipc/message.cc"
        "${LIBRARY_DIR}/ipc/metadata_internal.cc"
        "${LIBRARY_DIR}/ipc/options.cc"
        "${LIBRARY_DIR}/ipc/reader.cc"
        "${LIBRARY_DIR}/ipc/stream_to_file.cc"
        "${LIBRARY_DIR}/ipc/writer.cc"
        "${LIBRARY_DIR}/memory_pool.cc"
        "${LIBRARY_DIR}/pretty_print.cc"
        "${LIBRARY_DIR}/record_batch.cc"
        "${LIBRARY_DIR}/result.cc"
        "${LIBRARY_DIR}/scalar.cc"
        "${LIBRARY_DIR}/sparse_tensor.cc"
        "${LIBRARY_DIR}/status.cc"
        "${LIBRARY_DIR}/table.cc"
        "${LIBRARY_DIR}/table_builder.cc"
        "${LIBRARY_DIR}/tensor.cc"
        "${LIBRARY_DIR}/tensor/coo_converter.cc"
        "${LIBRARY_DIR}/tensor/csf_converter.cc"
        "${LIBRARY_DIR}/tensor/csx_converter.cc"
        "${LIBRARY_DIR}/type.cc"
        "${LIBRARY_DIR}/type_traits.cc"
        "${LIBRARY_DIR}/util/align_util.cc"
        "${LIBRARY_DIR}/util/async_util.cc"
        "${LIBRARY_DIR}/util/atfork_internal.cc"
        "${LIBRARY_DIR}/util/basic_decimal.cc"
        "${LIBRARY_DIR}/util/bit_block_counter.cc"
        "${LIBRARY_DIR}/util/bit_run_reader.cc"
        "${LIBRARY_DIR}/util/bit_util.cc"
        "${LIBRARY_DIR}/util/bitmap.cc"
        "${LIBRARY_DIR}/util/bitmap_builders.cc"
        "${LIBRARY_DIR}/util/bitmap_ops.cc"
        "${LIBRARY_DIR}/util/bpacking.cc"
        "${LIBRARY_DIR}/util/byte_size.cc"
        "${LIBRARY_DIR}/util/cancel.cc"
        "${LIBRARY_DIR}/util/compression.cc"
        "${LIBRARY_DIR}/util/counting_semaphore.cc"
        "${LIBRARY_DIR}/util/cpu_info.cc"
        "${LIBRARY_DIR}/util/crc32.cc"
        "${LIBRARY_DIR}/util/debug.cc"
        "${LIBRARY_DIR}/util/decimal.cc"
        "${LIBRARY_DIR}/util/delimiting.cc"
        "${LIBRARY_DIR}/util/dict_util.cc"
        "${LIBRARY_DIR}/util/float16.cc"
        "${LIBRARY_DIR}/util/formatting.cc"
        "${LIBRARY_DIR}/util/future.cc"
        "${LIBRARY_DIR}/util/hashing.cc"
        "${LIBRARY_DIR}/util/int_util.cc"
        "${LIBRARY_DIR}/util/io_util.cc"
        "${LIBRARY_DIR}/util/key_value_metadata.cc"
        "${LIBRARY_DIR}/util/list_util.cc"
        "${LIBRARY_DIR}/util/logging.cc"
        "${LIBRARY_DIR}/util/memory.cc"
        "${LIBRARY_DIR}/util/mutex.cc"
        "${LIBRARY_DIR}/util/ree_util.cc"
        "${LIBRARY_DIR}/util/string.cc"
        "${LIBRARY_DIR}/util/string_builder.cc"
        "${LIBRARY_DIR}/util/task_group.cc"
        "${LIBRARY_DIR}/util/tdigest.cc"
        "${LIBRARY_DIR}/util/thread_pool.cc"
        "${LIBRARY_DIR}/util/time.cc"
        "${LIBRARY_DIR}/util/tracing.cc"
        "${LIBRARY_DIR}/util/trie.cc"
        "${LIBRARY_DIR}/util/union_util.cc"
        "${LIBRARY_DIR}/util/unreachable.cc"
        "${LIBRARY_DIR}/util/uri.cc"
        "${LIBRARY_DIR}/util/utf8.cc"
        "${LIBRARY_DIR}/util/value_parsing.cc"
        "${LIBRARY_DIR}/vendored/base64.cpp"
        "${LIBRARY_DIR}/vendored/datetime/tz.cpp"
        "${LIBRARY_DIR}/vendored/double-conversion/bignum-dtoa.cc"
        "${LIBRARY_DIR}/vendored/double-conversion/bignum.cc"
        "${LIBRARY_DIR}/vendored/double-conversion/cached-powers.cc"
        "${LIBRARY_DIR}/vendored/double-conversion/double-to-string.cc"
        "${LIBRARY_DIR}/vendored/double-conversion/fast-dtoa.cc"
        "${LIBRARY_DIR}/vendored/double-conversion/fixed-dtoa.cc"
        "${LIBRARY_DIR}/vendored/double-conversion/string-to-double.cc"
        "${LIBRARY_DIR}/vendored/double-conversion/strtod.cc"
        "${LIBRARY_DIR}/vendored/musl/strptime.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriCommon.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriCompare.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriEscape.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriFile.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriIp4.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriIp4Base.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriMemory.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriNormalize.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriNormalizeBase.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriParse.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriParseBase.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriQuery.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriRecompose.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriResolve.c"
        "${LIBRARY_DIR}/vendored/uriparser/UriShorten.c"
        "${LIBRARY_DIR}/visitor.cc"

        "${ARROW_SRC_DIR}/arrow/adapters/orc/adapter.cc"
        "${ARROW_SRC_DIR}/arrow/adapters/orc/util.cc"
        "${ARROW_SRC_DIR}/arrow/adapters/orc/options.cc"
        )

add_definitions(-DARROW_WITH_LZ4)
SET(ARROW_SRCS "${LIBRARY_DIR}/util/compression_lz4.cc" ${ARROW_SRCS})

add_definitions(-DARROW_WITH_SNAPPY)
SET(ARROW_SRCS "${LIBRARY_DIR}/util/compression_snappy.cc" ${ARROW_SRCS})

add_definitions(-DARROW_WITH_ZLIB)
SET(ARROW_SRCS "${LIBRARY_DIR}/util/compression_zlib.cc" ${ARROW_SRCS})

add_definitions(-DARROW_WITH_ZSTD)
SET(ARROW_SRCS "${LIBRARY_DIR}/util/compression_zstd.cc" ${ARROW_SRCS})

add_definitions(-DARROW_WITH_BROTLI)
SET(ARROW_SRCS "${LIBRARY_DIR}/util/compression_brotli.cc" ${ARROW_SRCS})


add_library(_arrow ${ARROW_SRCS})

target_link_libraries(_arrow PRIVATE
    boost::filesystem

    _flatbuffers

    ch_contrib::double_conversion

    ch_contrib::lz4
    ch_contrib::snappy
    ch_contrib::zlib
    ch_contrib::zstd
    ch_contrib::brotli
)
target_link_libraries(_arrow PUBLIC _orc)

add_dependencies(_arrow protoc)

target_include_directories(_arrow SYSTEM BEFORE PUBLIC ${ARROW_SRC_DIR})
target_include_directories(_arrow SYSTEM BEFORE PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/arrow-cmake/cpp/src")

target_include_directories(_arrow SYSTEM PRIVATE ${ARROW_SRC_DIR})
target_include_directories(_arrow SYSTEM PRIVATE ${HDFS_INCLUDE_DIR})

# === parquet

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/parquet")
set(GEN_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/generated")
# arrow/cpp/src/parquet/CMakeLists.txt
set(PARQUET_SRCS
        "${LIBRARY_DIR}/arrow/path_internal.cc"
        "${LIBRARY_DIR}/arrow/reader.cc"
        "${LIBRARY_DIR}/arrow/reader_internal.cc"
        "${LIBRARY_DIR}/arrow/schema.cc"
        "${LIBRARY_DIR}/arrow/schema_internal.cc"
        "${LIBRARY_DIR}/arrow/writer.cc"
        "${LIBRARY_DIR}/benchmark_util.cc"
        "${LIBRARY_DIR}/bloom_filter.cc"
        "${LIBRARY_DIR}/bloom_filter_reader.cc"
        "${LIBRARY_DIR}/column_reader.cc"
        "${LIBRARY_DIR}/column_scanner.cc"
        "${LIBRARY_DIR}/column_writer.cc"
        "${LIBRARY_DIR}/encoding.cc"
        "${LIBRARY_DIR}/encryption/crypto_factory.cc"
        "${LIBRARY_DIR}/encryption/encryption.cc"
        "${LIBRARY_DIR}/encryption/encryption_internal.cc"
        "${LIBRARY_DIR}/encryption/encryption_internal_nossl.cc"
        "${LIBRARY_DIR}/encryption/file_key_unwrapper.cc"
        "${LIBRARY_DIR}/encryption/file_key_wrapper.cc"
        "${LIBRARY_DIR}/encryption/file_system_key_material_store.cc"
        "${LIBRARY_DIR}/encryption/internal_file_decryptor.cc"
        "${LIBRARY_DIR}/encryption/internal_file_encryptor.cc"
        "${LIBRARY_DIR}/encryption/key_material.cc"
        "${LIBRARY_DIR}/encryption/key_metadata.cc"
        "${LIBRARY_DIR}/encryption/key_toolkit.cc"
        "${LIBRARY_DIR}/encryption/key_toolkit_internal.cc"
        "${LIBRARY_DIR}/encryption/kms_client.cc"
        "${LIBRARY_DIR}/encryption/local_wrap_kms_client.cc"
        "${LIBRARY_DIR}/encryption/openssl_internal.cc"
        "${LIBRARY_DIR}/exception.cc"
        "${LIBRARY_DIR}/file_reader.cc"
        "${LIBRARY_DIR}/file_writer.cc"
        "${LIBRARY_DIR}/level_comparison.cc"
        "${LIBRARY_DIR}/level_comparison_avx2.cc"
        "${LIBRARY_DIR}/level_conversion.cc"
        "${LIBRARY_DIR}/level_conversion_bmi2.cc"
        "${LIBRARY_DIR}/metadata.cc"
        "${LIBRARY_DIR}/page_index.cc"
        "${LIBRARY_DIR}/platform.cc"
        "${LIBRARY_DIR}/printer.cc"
        "${LIBRARY_DIR}/properties.cc"
        "${LIBRARY_DIR}/schema.cc"
        "${LIBRARY_DIR}/statistics.cc"
        "${LIBRARY_DIR}/stream_reader.cc"
        "${LIBRARY_DIR}/stream_writer.cc"
        "${LIBRARY_DIR}/types.cc"
        "${LIBRARY_DIR}/xxhasher.cc"

        "${GEN_LIBRARY_DIR}/parquet_constants.cpp"
        "${GEN_LIBRARY_DIR}/parquet_types.cpp"
        )
#list(TRANSFORM PARQUET_SRCS PREPEND "${LIBRARY_DIR}/") # cmake 3.12
add_library(_parquet ${PARQUET_SRCS})
add_library(ch_contrib::parquet ALIAS _parquet)
target_include_directories(_parquet SYSTEM BEFORE
    PUBLIC
        "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src"
        "${CMAKE_CURRENT_SOURCE_DIR}/cpp/src")
target_link_libraries(_parquet
    PUBLIC
        _arrow
        ch_contrib::thrift
    PRIVATE
        boost::headers_only
        boost::regex
        OpenSSL::Crypto OpenSSL::SSL)

if (SANITIZE STREQUAL "undefined")
    target_compile_options(_parquet PRIVATE -fno-sanitize=undefined)
    target_compile_options(_arrow PRIVATE -fno-sanitize=undefined)
endif ()

# Define Thrift version for parquet (we use 0.16.0)
add_definitions(-DPARQUET_THRIFT_VERSION_MAJOR=0)
add_definitions(-DPARQUET_THRIFT_VERSION_MINOR=16)

# As per https://github.com/apache/arrow/pull/35672 you need to enable it explicitly.
add_definitions(-DARROW_ENABLE_THREADING)

# === tools

set(TOOLS_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/tools/parquet")
set(PARQUET_TOOLS parquet_dump_schema parquet_reader parquet_scan)
foreach (TOOL ${PARQUET_TOOLS})
    add_executable(${TOOL} "${TOOLS_DIR}/${TOOL}.cc")
    target_link_libraries(${TOOL} PRIVATE _parquet)
endforeach ()

# The library is large - avoid bloat.
if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options (_arrow PRIVATE -g0)
    target_compile_options (_parquet PRIVATE -g0)
endif()
