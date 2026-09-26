# libcudf, the RAPIDS dataframe library. CUDF_SOURCES below is upstream's `add_library(cudf ...)`
# list verbatim, so that a version bump is a diff against it; the exclusions come after.
# Dependencies come from contrib rather than CPM, and every compiled target goes through
# gpu_island_target - see cmake/cuda.cmake. Tests, benchmarks and the Python pieces are not built.

if (NOT ENABLE_GPU)
    message (STATUS "Not using cuDF")
    return ()
endif ()

set (CUDF_DIR "${ClickHouse_SOURCE_DIR}/contrib/cudf/cpp")

# version_config.hpp and logger_macros.hpp are checked in rather than generated - see
# contrib/rmm-cmake for how to regenerate them.
# Upstream's flags minus its -Werror CI policy. -diag-suppress=1407 silences the visibility
# warning CCCL templates trip in anonymous namespaces, as upstream's own build does.
set (CUDF_CUDA_FLAGS --expt-extended-lambda -diag-suppress=1407)

set (CUDF_EMBED_DIR "${CMAKE_CURRENT_BINARY_DIR}/rtcx_embed")
set (CUDF_INSTANTIATIONS_DIR "${CMAKE_CURRENT_BINARY_DIR}/instantiations")

# ##################################################################################################
# JIT embed
#
# cuDF compiles part of itself at query time; librtcx's embed helpers pack the headers NVRTC needs
# into an object file. The helpers look up `zstd` and `xxhash` by literal target name, hence the
# aliases below.
# ##################################################################################################

add_library (zstd ALIAS _zstd)
add_library (xxhash ALIAS xxHash)
set (ZSTD_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/zstd/lib")

include ("${ClickHouse_SOURCE_DIR}/contrib/librtcx/cmake/modules/embed.cmake")

# rtcx_embed() adds a host tool to write the blob. It cannot go through ClickHouse's C++ pipeline:
# rtcx's sources rely on transitive standard-library includes and unbounded iterators, which this
# libc++ is configured against - so the generator joins the island too.
function (cudf_embed_runner_island _runner)
    gpu_island_executable (${_runner})
    target_include_directories (${_runner} SYSTEM PRIVATE
        "${ClickHouse_SOURCE_DIR}/contrib/librtcx/include"
        "${ClickHouse_SOURCE_DIR}/contrib/zstd/lib"
        "${ClickHouse_SOURCE_DIR}/contrib/xxHash")
    target_compile_definitions (${_runner} PRIVATE XXH_INLINE_ALL)
    # rtcx/embed.hpp uses std::variant without including <variant>; libstdc++ 15 no longer
    # provides it transitively through <map> or <string>, so pre-include it here.
    target_compile_options (${_runner} PRIVATE -include variant)
    # By file, not by target: linking ch_contrib::zstd would bring global-group's libc++ and
    # friends into a libstdc++ program. See gpu_island_executable in cmake/cuda.cmake.
    target_link_libraries (${_runner} PRIVATE "$<TARGET_FILE:ch_contrib::zstd>" ${CMAKE_DL_LIBS})
    add_dependencies (${_runner} _zstd)
endfunction ()

rtcx_add_embed (cudf_cuda_embed)

rtcx_embed_includes (cudf_cuda_embed
    SOURCE_DIRECTORY "${ClickHouse_SOURCE_DIR}/contrib/librtcx/libcxx"
    DEST_DIRECTORY librtcx/libcxx
    INCLUDE_DIRECTORIES librtcx/libcxx)

rtcx_embed_includes (cudf_cuda_embed
    SOURCE_DIRECTORY "${CUDF_DIR}/include/cudf"
    DEST_DIRECTORY cudf/cpp/include/cudf
    INCLUDE_DIRECTORIES cudf/cpp/include)

foreach (_jit_dir IN ITEMS jit binaryop/jit join/jit rolling transform/jit)
    rtcx_embed_includes (cudf_cuda_embed
        SOURCE_DIRECTORY "${CUDF_DIR}/src/${_jit_dir}"
        DEST_DIRECTORY "cudf/cpp/src/${_jit_dir}"
        INCLUDE_DIRECTORIES cudf/cpp/src)
endforeach ()

# ClickHouse's cccl wrapper carries all three CCCL libraries on one target, so libcudacxx's
# include directory is named directly where upstream walks the target's interface.
rtcx_embed_includes (cudf_cuda_embed
    SOURCE_DIRECTORY "${ClickHouse_SOURCE_DIR}/contrib/cccl/libcudacxx/include"
    DEST_DIRECTORY CCCL/libcudacxx/include
    INCLUDE_DIRECTORIES CCCL/libcudacxx/include)

rtcx_embed (cudf_cuda_embed COMPRESSION zstd OUTPUT_DIRECTORY "${CUDF_EMBED_DIR}")

cudf_embed_runner_island (cudf_cuda_embed__jit_embed_run)

# ##################################################################################################
# Precompiled JIT fragments
#
# Transform kernels for the types below are compiled ahead of time so the common cases skip NVRTC.
# Upstream's AddFragment.cmake macro is inlined here, because a fragment has to become an island
# target before anything else is set on it.
# ##################################################################################################

set (CUDF_PRECOMPILE_PHYSICAL_TYPES
    uint8_t uint16_t uint32_t uint64_t
    numeric::decimal32 numeric::decimal64 numeric::decimal128)

rtcx_add_embed (cudf_fragments)

# One fragment: an OBJECT library that compiles src/transform/jit/kernel.cu down to a fatbin
# for a single kernel instantiation, handed to the kernel through two generated headers.
function (cudf_add_fragment FRAGMENT INSTANCE UDF_TYPE FILE_INDEX)
    set (_object "cudf_fragments_${FRAGMENT}")
    add_library (${_object} OBJECT "${CUDF_DIR}/src/transform/jit/kernel.cu")

    # First, before any of the properties below: this wipes the target clean.
    gpu_island_target (${_object})

    set (_instantiation_dir "${CUDF_INSTANTIATIONS_DIR}/${FRAGMENT}")
    file (GENERATE OUTPUT "${_instantiation_dir}/cudf/detail/kernel_instance.cuh"
        CONTENT "#pragma once\n#define CUDF_KERNEL_INSTANCE ${INSTANCE}\n")
    file (GENERATE OUTPUT "${_instantiation_dir}/cudf/detail/operation_udf.cuh"
        CONTENT "#pragma once\n#define CUDF_UDF_TYPE ${UDF_TYPE}\n")

    target_include_directories (${_object} SYSTEM PRIVATE
        "${_instantiation_dir}"
        "${CUDF_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CUDF_DIR}/src"
        "${GPU_CUDA_ROOT}/include")

    target_compile_definitions (${_object} PRIVATE CUDF_DISABLE_EXPORTS CUDF_LTO_MODE)
    target_compile_options (${_object} PRIVATE ${CUDF_CUDA_FLAGS} --compress-mode=size)

    set_target_properties (${_object} PROPERTIES
        CUDA_SEPARABLE_COMPILATION ON
        CUDA_FATBIN_COMPILATION ON
        INTERPROCEDURAL_OPTIMIZATION ON
        POSITION_INDEPENDENT_CODE ON
        CUDA_STANDARD 20
        CUDA_STANDARD_REQUIRED ON
        CUDA_VISIBILITY_PRESET hidden)

    # Island or header-only dependencies only: the IO libraries upstream also names here would hand
    # this target global-group's compile options, and the JIT kernel does not include them.
    target_link_libraries (${_object} PRIVATE
        ch_contrib::cccl
        ch_contrib::rmm
        ch_contrib::rapids_logger
        ch_contrib::bs_thread_pool
        ch_contrib::nvtx3
        ch_contrib::cuco
        ch_gpu::cudart)

    rtcx_embed_blob (cudf_fragments
        FILE $<TARGET_OBJECTS:${_object}>
        DEST "fragments/${FRAGMENT}.fatbin"
        ID ${FRAGMENT}
        ARRAY_IDS transform_kernel_FILE_INDEX transform_kernel_INSTANCE
        ARRAY_VALUES ${FILE_INDEX} "${INSTANCE}")
endfunction ()

# The instantiations, exactly the set upstream precompiles: one input column, then two with the
# right-hand side as a column and as a scalar.
foreach (_type IN LISTS CUDF_PRECOMPILE_PHYSICAL_TYPES)
    get_property (_file_index TARGET cudf_fragments__embed_props PROPERTY EMBED_FILE_INDEX)
    cudf_add_fragment ("transform_kernel_${_file_index}"
        "cudf::jit::transform_kernel<false, false, cudf::jit::type_list<cudf::jit::column_accessor<0ULL, cudf::column_device_view_core, ${_type}, false, 0>>, cudf::jit::type_list<cudf::jit::column_accessor<0ULL, cudf::mutable_column_device_view_core, ${_type}, false, 0>>>"
        "int(${_type} *, ${_type})"
        "${_file_index}")
endforeach ()

foreach (_type IN LISTS CUDF_PRECOMPILE_PHYSICAL_TYPES)
    foreach (_rhs_is_scalar IN ITEMS "false" "true")
        get_property (_file_index TARGET cudf_fragments__embed_props PROPERTY EMBED_FILE_INDEX)
        cudf_add_fragment ("transform_kernel_${_file_index}"
            "cudf::jit::transform_kernel<false, false, cudf::jit::type_list<cudf::jit::column_accessor<0ULL, cudf::column_device_view_core, ${_type}, false, 0>, cudf::jit::column_accessor<1ULL, cudf::column_device_view_core, ${_type}, ${_rhs_is_scalar}, 0>>, cudf::jit::type_list<cudf::jit::column_accessor<0ULL, cudf::mutable_column_device_view_core, ${_type}, false, 0>>>"
            "int(${_type} *, ${_type}, ${_type})"
            "${_file_index}")
    endforeach ()
endforeach ()

rtcx_embed (cudf_fragments COMPRESSION none OUTPUT_DIRECTORY "${CUDF_EMBED_DIR}")

cudf_embed_runner_island (cudf_fragments__jit_embed_run)

# ##################################################################################################
# The library
# ##################################################################################################

# ##################################################################################################
# What is left out
#
# cuDF's format readers and writers are not built - nothing here reads a file through cuDF, and two
# of them do not compile against this contrib anyway (`io/parquet/arrow_schema_writer.cpp`
# static-asserts on a different flatbuffers, `io/orc/writer_impl.cu` hits an overload ambiguity
# under CUDA 13.2). `io/comp` and `io/utilities` stay: the rest of the library links against them.
# ##################################################################################################

set (CUDF_SOURCES
    "${CUDF_DIR}/src/aggregation/aggregation.cpp"
    "${CUDF_DIR}/src/aggregation/aggregation.cu"
    "${CUDF_DIR}/src/aggregation/result_cache.cpp"
    "${CUDF_DIR}/src/ast/expression_parser.cpp"
    "${CUDF_DIR}/src/ast/expressions.cpp"
    "${CUDF_DIR}/src/ast/jit/expressions.cpp"
    "${CUDF_DIR}/src/ast/operators.cpp"
    "${CUDF_DIR}/src/binaryop/binaryop.cpp"
    "${CUDF_DIR}/src/binaryop/compiled/ATan2.cu"
    "${CUDF_DIR}/src/binaryop/compiled/Add.cu"
    "${CUDF_DIR}/src/binaryop/compiled/BitwiseAnd.cu"
    "${CUDF_DIR}/src/binaryop/compiled/BitwiseOr.cu"
    "${CUDF_DIR}/src/binaryop/compiled/BitwiseXor.cu"
    "${CUDF_DIR}/src/binaryop/compiled/Div.cu"
    "${CUDF_DIR}/src/binaryop/compiled/FloorDiv.cu"
    "${CUDF_DIR}/src/binaryop/compiled/Greater.cu"
    "${CUDF_DIR}/src/binaryop/compiled/GreaterEqual.cu"
    "${CUDF_DIR}/src/binaryop/compiled/IntPow.cu"
    "${CUDF_DIR}/src/binaryop/compiled/Less.cu"
    "${CUDF_DIR}/src/binaryop/compiled/LessEqual.cu"
    "${CUDF_DIR}/src/binaryop/compiled/LogBase.cu"
    "${CUDF_DIR}/src/binaryop/compiled/LogicalAnd.cu"
    "${CUDF_DIR}/src/binaryop/compiled/LogicalOr.cu"
    "${CUDF_DIR}/src/binaryop/compiled/Mod.cu"
    "${CUDF_DIR}/src/binaryop/compiled/Mul.cu"
    "${CUDF_DIR}/src/binaryop/compiled/NullEquals.cu"
    "${CUDF_DIR}/src/binaryop/compiled/NullNotEquals.cu"
    "${CUDF_DIR}/src/binaryop/compiled/NullLogicalAnd.cu"
    "${CUDF_DIR}/src/binaryop/compiled/NullLogicalOr.cu"
    "${CUDF_DIR}/src/binaryop/compiled/NullMax.cu"
    "${CUDF_DIR}/src/binaryop/compiled/NullMin.cu"
    "${CUDF_DIR}/src/binaryop/compiled/PMod.cu"
    "${CUDF_DIR}/src/binaryop/compiled/Pow.cu"
    "${CUDF_DIR}/src/binaryop/compiled/PyMod.cu"
    "${CUDF_DIR}/src/binaryop/compiled/ShiftLeft.cu"
    "${CUDF_DIR}/src/binaryop/compiled/ShiftRight.cu"
    "${CUDF_DIR}/src/binaryop/compiled/ShiftRightUnsigned.cu"
    "${CUDF_DIR}/src/binaryop/compiled/Sub.cu"
    "${CUDF_DIR}/src/binaryop/compiled/TrueDiv.cu"
    "${CUDF_DIR}/src/binaryop/compiled/binary_ops.cu"
    "${CUDF_DIR}/src/binaryop/compiled/equality_ops.cu"
    "${CUDF_DIR}/src/binaryop/compiled/util.cpp"
    "${CUDF_DIR}/src/labeling/label_bins.cu"
    "${CUDF_DIR}/src/bitmask/null_mask.cu"
    "${CUDF_DIR}/src/bitmask/is_element_valid.cpp"
    "${CUDF_DIR}/src/column/column.cu"
    "${CUDF_DIR}/src/column/column_device_view.cu"
    "${CUDF_DIR}/src/column/column_factories.cpp"
    "${CUDF_DIR}/src/column/column_factories.cu"
    "${CUDF_DIR}/src/column/column_view.cpp"
    "${CUDF_DIR}/src/copying/concatenate.cu"
    "${CUDF_DIR}/src/copying/contiguous_split.cu"
    "${CUDF_DIR}/src/copying/copy.cpp"
    "${CUDF_DIR}/src/copying/copy.cu"
    "${CUDF_DIR}/src/copying/copy_range.cu"
    "${CUDF_DIR}/src/copying/gather.cu"
    "${CUDF_DIR}/src/copying/get_element.cu"
    "${CUDF_DIR}/src/copying/pack.cpp"
    "${CUDF_DIR}/src/copying/purge_nonempty_nulls.cu"
    "${CUDF_DIR}/src/copying/reverse.cu"
    "${CUDF_DIR}/src/copying/sample.cu"
    "${CUDF_DIR}/src/copying/scatter.cu"
    "${CUDF_DIR}/src/copying/shift.cu"
    "${CUDF_DIR}/src/copying/slice.cu"
    "${CUDF_DIR}/src/copying/split.cpp"
    "${CUDF_DIR}/src/copying/segmented_shift.cu"
    "${CUDF_DIR}/src/datetime/datetime_ops.cu"
    "${CUDF_DIR}/src/dictionary/add_keys.cu"
    "${CUDF_DIR}/src/dictionary/decode.cu"
    "${CUDF_DIR}/src/dictionary/detail/concatenate.cu"
    "${CUDF_DIR}/src/dictionary/detail/merge.cu"
    "${CUDF_DIR}/src/dictionary/dictionary_column_view.cpp"
    "${CUDF_DIR}/src/dictionary/dictionary_factories.cu"
    "${CUDF_DIR}/src/dictionary/encode.cu"
    "${CUDF_DIR}/src/dictionary/match_keys.cu"
    "${CUDF_DIR}/src/dictionary/remove_keys.cu"
    "${CUDF_DIR}/src/dictionary/replace.cu"
    "${CUDF_DIR}/src/dictionary/search.cu"
    "${CUDF_DIR}/src/dictionary/set_keys.cu"
    "${CUDF_DIR}/src/filling/calendrical_month_sequence.cu"
    "${CUDF_DIR}/src/filling/fill.cu"
    "${CUDF_DIR}/src/filling/repeat.cu"
    "${CUDF_DIR}/src/filling/sequence.cu"
    "${CUDF_DIR}/src/groupby/common/m2_var_std.cu"
    "${CUDF_DIR}/src/groupby/common/utils.cpp"
    "${CUDF_DIR}/src/groupby/groupby.cu"
    "${CUDF_DIR}/src/groupby/hash/compute_global_memory_aggs.cu"
    "${CUDF_DIR}/src/groupby/hash/compute_global_memory_aggs_null.cu"
    "${CUDF_DIR}/src/groupby/hash/compute_groupby.cu"
    "${CUDF_DIR}/src/groupby/hash/compute_mapping_indices.cu"
    "${CUDF_DIR}/src/groupby/hash/compute_mapping_indices_null.cu"
    "${CUDF_DIR}/src/groupby/hash/compute_shared_memory_aggs.cu"
    "${CUDF_DIR}/src/groupby/hash/compute_single_pass_aggs.cu"
    "${CUDF_DIR}/src/groupby/hash/compute_single_pass_aggs_null.cu"
    "${CUDF_DIR}/src/groupby/hash/extract_single_pass_aggs.cpp"
    "${CUDF_DIR}/src/groupby/hash/groupby.cu"
    "${CUDF_DIR}/src/groupby/hash/hash_compound_agg_finalizer.cu"
    "${CUDF_DIR}/src/groupby/hash/output_utils.cu"
    "${CUDF_DIR}/src/groupby/sort/aggregate.cpp"
    "${CUDF_DIR}/src/groupby/sort/group_argmax.cu"
    "${CUDF_DIR}/src/groupby/sort/group_argmin.cu"
    "${CUDF_DIR}/src/groupby/sort/group_bitwise.cu"
    "${CUDF_DIR}/src/groupby/sort/group_collect.cu"
    "${CUDF_DIR}/src/groupby/sort/group_correlation.cu"
    "${CUDF_DIR}/src/groupby/sort/group_count.cu"
    "${CUDF_DIR}/src/groupby/sort/group_count_scan.cu"
    "${CUDF_DIR}/src/groupby/sort/group_histogram.cu"
    "${CUDF_DIR}/src/groupby/sort/group_m2.cu"
    "${CUDF_DIR}/src/groupby/sort/group_max.cu"
    "${CUDF_DIR}/src/groupby/sort/group_max_scan.cu"
    "${CUDF_DIR}/src/groupby/sort/group_merge_lists.cu"
    "${CUDF_DIR}/src/groupby/sort/group_merge_m2.cu"
    "${CUDF_DIR}/src/groupby/sort/group_min.cu"
    "${CUDF_DIR}/src/groupby/sort/group_min_scan.cu"
    "${CUDF_DIR}/src/groupby/sort/group_nth_element.cu"
    "${CUDF_DIR}/src/groupby/sort/group_nunique.cu"
    "${CUDF_DIR}/src/groupby/sort/group_product.cu"
    "${CUDF_DIR}/src/groupby/sort/group_product_scan.cu"
    "${CUDF_DIR}/src/groupby/sort/group_quantiles.cu"
    "${CUDF_DIR}/src/groupby/sort/group_rank_scan.cu"
    "${CUDF_DIR}/src/groupby/sort/group_replace_nulls.cu"
    "${CUDF_DIR}/src/groupby/sort/group_std.cu"
    "${CUDF_DIR}/src/groupby/sort/group_sum.cu"
    "${CUDF_DIR}/src/groupby/sort/group_sum_overflow.cu"
    "${CUDF_DIR}/src/groupby/sort/group_sum_scan.cu"
    "${CUDF_DIR}/src/groupby/sort/group_topk.cu"
    "${CUDF_DIR}/src/groupby/sort/host_udf_aggregation.cpp"
    "${CUDF_DIR}/src/groupby/sort/scan.cpp"
    "${CUDF_DIR}/src/groupby/sort/sort_helper.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby.cpp"
    "${CUDF_DIR}/src/groupby/streaming_groupby/aggregate.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/impl.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/insert.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/insert_first.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/insert_first_nested.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/insert_nested.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/insert_subsequent.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/insert_subsequent_nested.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/merge.cu"
    "${CUDF_DIR}/src/hash/md5_hash.cu"
    "${CUDF_DIR}/src/hash/murmurhash3_x86_32.cu"
    "${CUDF_DIR}/src/hash/murmurhash3_x64_128.cu"
    "${CUDF_DIR}/src/hash/sha1_hash.cu"
    "${CUDF_DIR}/src/hash/sha224_hash.cu"
    "${CUDF_DIR}/src/hash/sha256_hash.cu"
    "${CUDF_DIR}/src/hash/sha384_hash.cu"
    "${CUDF_DIR}/src/hash/sha512_hash.cu"
    "${CUDF_DIR}/src/hash/xxhash_32.cu"
    "${CUDF_DIR}/src/hash/xxhash_64.cu"
    "${CUDF_DIR}/src/io/comp/brotli_dict.cpp"
    "${CUDF_DIR}/src/io/comp/compression.cpp"
    "${CUDF_DIR}/src/io/comp/compression.cu"
    "${CUDF_DIR}/src/io/comp/common.cpp"
    "${CUDF_DIR}/src/io/comp/cpu_unbz2.cpp"
    "${CUDF_DIR}/src/io/comp/debrotli.cu"
    "${CUDF_DIR}/src/io/comp/gpuinflate.cu"
    "${CUDF_DIR}/src/io/comp/nvcomp_adapter.cpp"
    "${CUDF_DIR}/src/io/comp/nvcomp_adapter.cu"
    "${CUDF_DIR}/src/io/comp/snap.cu"
    "${CUDF_DIR}/src/io/comp/decompression.cpp"
    "${CUDF_DIR}/src/io/comp/unsnap.cu"
    "${CUDF_DIR}/src/datetime/timezone.cpp"
    "${CUDF_DIR}/src/io/utilities/base64_utilities.cpp"
    "${CUDF_DIR}/src/io/utilities/column_buffer.cpp"
    "${CUDF_DIR}/src/io/utilities/column_buffer_strings.cu"
    "${CUDF_DIR}/src/io/utilities/config_utils.cpp"
    "${CUDF_DIR}/src/io/utilities/data_casting.cu"
    "${CUDF_DIR}/src/io/utilities/data_sink.cpp"
    "${CUDF_DIR}/src/io/utilities/datasource.cpp"
    "${CUDF_DIR}/src/io/utilities/row_selection.cpp"
    "${CUDF_DIR}/src/io/utilities/type_inference.cu"
    "${CUDF_DIR}/src/io/utilities/trie.cu"
    "${CUDF_DIR}/src/jit/cache.cpp"
    "${CUDF_DIR}/src/jit/helpers.cpp"
    "${CUDF_DIR}/src/jit/parser.cpp"
    "${CUDF_DIR}/src/jit/row_ir.cpp"
    "${CUDF_DIR}/src/jit/util.cpp"
    "${CUDF_DIR}/src/join/conditional_join.cu"
    "${CUDF_DIR}/src/join/cross_join.cu"
    "${CUDF_DIR}/src/join/distinct_hash_join.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_jit.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_kernel_complex.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_kernel_null_complex.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_kernel_null_primitive.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_kernel_primitive.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_output_size_kernel_complex.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_output_size_kernel_null_complex.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_output_size_kernel_null_primitive.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_output_size_kernel_primitive.cu"
    "${CUDF_DIR}/src/join/filtered_join.cu"
    "${CUDF_DIR}/src/join/hash_join/finalize_partitioned_full_join.cpp"
    "${CUDF_DIR}/src/join/hash_join/full_join_match_context.cpp"
    "${CUDF_DIR}/src/join/hash_join/full_join_retrieve.cu"
    "${CUDF_DIR}/src/join/hash_join/full_join_size.cu"
    "${CUDF_DIR}/src/join/hash_join/full_join_size_impl.cu"
    "${CUDF_DIR}/src/join/hash_join/hash_join.cu"
    "${CUDF_DIR}/src/join/hash_join/inner_join_match_context.cpp"
    "${CUDF_DIR}/src/join/hash_join/inner_join_retrieve.cu"
    "${CUDF_DIR}/src/join/hash_join/inner_join_size.cu"
    "${CUDF_DIR}/src/join/hash_join/left_join_match_context.cpp"
    "${CUDF_DIR}/src/join/hash_join/left_join_retrieve.cu"
    "${CUDF_DIR}/src/join/hash_join/left_join_size.cu"
    "${CUDF_DIR}/src/join/hash_join/match_context.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_count.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_count_outer.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_full_join.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_inner_join.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_join_retrieve.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_left_join.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_retrieve.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_retrieve_outer.cu"
    "${CUDF_DIR}/src/join/join.cu"
    "${CUDF_DIR}/src/join/join_utils.cu"
    "${CUDF_DIR}/src/join/key_remapping.cu"
    "${CUDF_DIR}/src/join/mark_join.cu"
    "${CUDF_DIR}/src/join/mixed_join.cu"
    "${CUDF_DIR}/src/join/mixed_join_kernels_semi.cu"
    "${CUDF_DIR}/src/join/mixed_join_semi.cu"
    "${CUDF_DIR}/src/join/sort_merge_join.cu"
    "${CUDF_DIR}/src/json/json_path.cu"
    "${CUDF_DIR}/src/lists/contains.cu"
    "${CUDF_DIR}/src/lists/combine/concatenate_list_elements.cu"
    "${CUDF_DIR}/src/lists/combine/concatenate_rows.cu"
    "${CUDF_DIR}/src/lists/copying/concatenate.cu"
    "${CUDF_DIR}/src/lists/copying/copying.cu"
    "${CUDF_DIR}/src/lists/copying/gather.cu"
    "${CUDF_DIR}/src/lists/copying/segmented_gather.cu"
    "${CUDF_DIR}/src/lists/copying/scatter_helper.cu"
    "${CUDF_DIR}/src/lists/count_elements.cu"
    "${CUDF_DIR}/src/lists/dremel.cu"
    "${CUDF_DIR}/src/lists/explode.cu"
    "${CUDF_DIR}/src/lists/extract.cu"
    "${CUDF_DIR}/src/lists/interleave_columns.cu"
    "${CUDF_DIR}/src/lists/lists_column_factories.cu"
    "${CUDF_DIR}/src/lists/lists_column_view.cu"
    "${CUDF_DIR}/src/lists/reverse.cu"
    "${CUDF_DIR}/src/lists/segmented_sort.cu"
    "${CUDF_DIR}/src/lists/sequences.cu"
    "${CUDF_DIR}/src/lists/set_operations.cu"
    "${CUDF_DIR}/src/lists/stream_compaction/apply_boolean_mask.cu"
    "${CUDF_DIR}/src/lists/stream_compaction/distinct.cu"
    "${CUDF_DIR}/src/lists/utilities.cu"
    "${CUDF_DIR}/src/merge/merge.cu"
    "${CUDF_DIR}/src/partitioning/partitioning.cu"
    "${CUDF_DIR}/src/partitioning/round_robin.cu"
    "${CUDF_DIR}/src/quantiles/tdigest/tdigest.cu"
    "${CUDF_DIR}/src/quantiles/tdigest/tdigest_aggregation.cu"
    "${CUDF_DIR}/src/quantiles/tdigest/tdigest_column_view.cpp"
    "${CUDF_DIR}/src/quantiles/quantile.cu"
    "${CUDF_DIR}/src/quantiles/quantiles.cu"
    "${CUDF_DIR}/src/reductions/all.cu"
    "${CUDF_DIR}/src/reductions/any.cu"
    "${CUDF_DIR}/src/reductions/approx_distinct_count.cu"
    "${CUDF_DIR}/src/reductions/argmax.cu"
    "${CUDF_DIR}/src/reductions/argmin.cu"
    "${CUDF_DIR}/src/reductions/bitwise.cu"
    "${CUDF_DIR}/src/reductions/collect_ops.cu"
    "${CUDF_DIR}/src/reductions/count.cpp"
    "${CUDF_DIR}/src/reductions/distinct_count.cu"
    "${CUDF_DIR}/src/reductions/histogram.cu"
    "${CUDF_DIR}/src/reductions/max.cu"
    "${CUDF_DIR}/src/reductions/mean.cu"
    "${CUDF_DIR}/src/reductions/min.cu"
    "${CUDF_DIR}/src/reductions/minmax.cu"
    "${CUDF_DIR}/src/reductions/nth_element.cu"
    "${CUDF_DIR}/src/reductions/nunique.cpp"
    "${CUDF_DIR}/src/reductions/product.cu"
    "${CUDF_DIR}/src/reductions/quantile.cu"
    "${CUDF_DIR}/src/reductions/reductions.cpp"
    "${CUDF_DIR}/src/reductions/scan/ewm.cu"
    "${CUDF_DIR}/src/reductions/scan/rank_scan.cu"
    "${CUDF_DIR}/src/reductions/scan/scan.cpp"
    "${CUDF_DIR}/src/reductions/scan/scan_exclusive.cu"
    "${CUDF_DIR}/src/reductions/scan/scan_inclusive.cu"
    "${CUDF_DIR}/src/reductions/segmented/all.cu"
    "${CUDF_DIR}/src/reductions/segmented/any.cu"
    "${CUDF_DIR}/src/reductions/segmented/counts.cu"
    "${CUDF_DIR}/src/reductions/segmented/max.cu"
    "${CUDF_DIR}/src/reductions/segmented/mean.cu"
    "${CUDF_DIR}/src/reductions/segmented/min.cu"
    "${CUDF_DIR}/src/reductions/segmented/nunique.cu"
    "${CUDF_DIR}/src/reductions/segmented/product.cu"
    "${CUDF_DIR}/src/reductions/segmented/reductions.cpp"
    "${CUDF_DIR}/src/reductions/segmented/std.cu"
    "${CUDF_DIR}/src/reductions/segmented/sum.cu"
    "${CUDF_DIR}/src/reductions/segmented/sum_of_squares.cu"
    "${CUDF_DIR}/src/reductions/segmented/update_validity.cu"
    "${CUDF_DIR}/src/reductions/segmented/var.cu"
    "${CUDF_DIR}/src/reductions/std.cu"
    "${CUDF_DIR}/src/reductions/sum.cu"
    "${CUDF_DIR}/src/reductions/sum_of_squares.cu"
    "${CUDF_DIR}/src/reductions/sum_overflow.cu"
    "${CUDF_DIR}/src/reductions/unique_count.cu"
    "${CUDF_DIR}/src/reductions/unique_count_column.cu"
    "${CUDF_DIR}/src/reductions/var.cu"
    "${CUDF_DIR}/src/replace/clamp.cu"
    "${CUDF_DIR}/src/replace/nans.cu"
    "${CUDF_DIR}/src/replace/nulls.cu"
    "${CUDF_DIR}/src/replace/replace.cu"
    "${CUDF_DIR}/src/reshape/byte_cast.cu"
    "${CUDF_DIR}/src/reshape/interleave_columns.cu"
    "${CUDF_DIR}/src/reshape/table_to_array.cu"
    "${CUDF_DIR}/src/reshape/tile.cu"
    "${CUDF_DIR}/src/rolling/detail/optimized_unbounded_window.cpp"
    "${CUDF_DIR}/src/rolling/detail/rolling_collect_list.cu"
    "${CUDF_DIR}/src/rolling/detail/rolling_fixed_window.cu"
    "${CUDF_DIR}/src/rolling/detail/rolling_utils.cu"
    "${CUDF_DIR}/src/rolling/detail/rolling_variable_window.cu"
    "${CUDF_DIR}/src/rolling/grouped_rolling.cu"
    "${CUDF_DIR}/src/rolling/range_rolling.cu"
    "${CUDF_DIR}/src/rolling/range_window_bounds.cpp"
    "${CUDF_DIR}/src/rolling/rolling.cpp"
    "${CUDF_DIR}/src/round/round.cu"
    "${CUDF_DIR}/src/row_operator/primitive_row_operators.cu"
    "${CUDF_DIR}/src/row_operator/row_operators.cu"
    "${CUDF_DIR}/src/runtime/context.cpp"
    "${CUDF_DIR}/src/scalar/scalar.cpp"
    "${CUDF_DIR}/src/scalar/scalar_factories.cpp"
    "${CUDF_DIR}/src/search/contains_column.cu"
    "${CUDF_DIR}/src/search/contains_scalar.cu"
    "${CUDF_DIR}/src/search/contains_table.cu"
    "${CUDF_DIR}/src/search/contains_table_impl.cu"
    "${CUDF_DIR}/src/search/contains_table_impl_nested.cu"
    "${CUDF_DIR}/src/search/contains_table_impl_primitive.cu"
    "${CUDF_DIR}/src/search/search_ordered.cu"
    "${CUDF_DIR}/src/sort/is_sorted.cu"
    "${CUDF_DIR}/src/sort/rank.cu"
    "${CUDF_DIR}/src/sort/segmented_sort.cu"
    "${CUDF_DIR}/src/sort/segmented_top_k.cu"
    "${CUDF_DIR}/src/sort/sort.cu"
    "${CUDF_DIR}/src/sort/sort_column.cu"
    "${CUDF_DIR}/src/sort/sort_radix.cu"
    "${CUDF_DIR}/src/sort/sorted_order_radix.cu"
    "${CUDF_DIR}/src/sort/stable_segmented_sort.cu"
    "${CUDF_DIR}/src/sort/stable_sort_column.cu"
    "${CUDF_DIR}/src/sort/stable_sort.cu"
    "${CUDF_DIR}/src/sort/top_k.cu"
    "${CUDF_DIR}/src/stream_compaction/apply_boolean_mask.cu"
    "${CUDF_DIR}/src/stream_compaction/distinct.cu"
    "${CUDF_DIR}/src/stream_compaction/distinct_helpers.cu"
    "${CUDF_DIR}/src/stream_compaction/drop_nans.cu"
    "${CUDF_DIR}/src/stream_compaction/drop_nulls.cu"
    "${CUDF_DIR}/src/stream_compaction/filter/filter.cu"
    "${CUDF_DIR}/src/stream_compaction/stable_distinct.cu"
    "${CUDF_DIR}/src/stream_compaction/unique.cu"
    "${CUDF_DIR}/src/strings/attributes.cu"
    "${CUDF_DIR}/src/strings/capitalize.cu"
    "${CUDF_DIR}/src/strings/case.cu"
    "${CUDF_DIR}/src/strings/char_types/char_cases.cu"
    "${CUDF_DIR}/src/strings/char_types/char_types.cu"
    "${CUDF_DIR}/src/strings/combine/concatenate.cu"
    "${CUDF_DIR}/src/strings/combine/join.cu"
    "${CUDF_DIR}/src/strings/combine/join_list_elements.cu"
    "${CUDF_DIR}/src/strings/contains.cu"
    "${CUDF_DIR}/src/strings/convert/convert_booleans.cu"
    "${CUDF_DIR}/src/strings/convert/convert_datetime.cu"
    "${CUDF_DIR}/src/strings/convert/convert_durations.cu"
    "${CUDF_DIR}/src/strings/convert/convert_fixed_point.cu"
    "${CUDF_DIR}/src/strings/convert/convert_floats.cu"
    "${CUDF_DIR}/src/strings/convert/convert_hex.cu"
    "${CUDF_DIR}/src/strings/convert/convert_integers.cu"
    "${CUDF_DIR}/src/strings/convert/convert_ipv4.cu"
    "${CUDF_DIR}/src/strings/convert/convert_urls.cu"
    "${CUDF_DIR}/src/strings/convert/convert_lists.cu"
    "${CUDF_DIR}/src/strings/convert/int_cast.cu"
    "${CUDF_DIR}/src/strings/copying/concatenate.cu"
    "${CUDF_DIR}/src/strings/copying/copying.cu"
    "${CUDF_DIR}/src/strings/copying/copy_range.cu"
    "${CUDF_DIR}/src/strings/copying/shift.cu"
    "${CUDF_DIR}/src/strings/count_matches.cu"
    "${CUDF_DIR}/src/strings/extract/extract.cu"
    "${CUDF_DIR}/src/strings/extract/extract_all.cu"
    "${CUDF_DIR}/src/strings/filling/fill.cu"
    "${CUDF_DIR}/src/strings/filter_chars.cu"
    "${CUDF_DIR}/src/strings/like.cu"
    "${CUDF_DIR}/src/strings/merge/merge.cu"
    "${CUDF_DIR}/src/strings/padding.cu"
    "${CUDF_DIR}/src/strings/positions.cu"
    "${CUDF_DIR}/src/strings/regex/gkexec.cpp"
    "${CUDF_DIR}/src/strings/regex/glushkov_regcomp.cpp"
    "${CUDF_DIR}/src/strings/regex/regcomp.cpp"
    "${CUDF_DIR}/src/strings/regex/regexec.cpp"
    "${CUDF_DIR}/src/strings/regex/regex_program.cpp"
    "${CUDF_DIR}/src/strings/repeat_strings.cu"
    "${CUDF_DIR}/src/strings/replace/backref_re.cu"
    "${CUDF_DIR}/src/strings/replace/find_replace.cu"
    "${CUDF_DIR}/src/strings/replace/multi.cu"
    "${CUDF_DIR}/src/strings/replace/replace.cu"
    "${CUDF_DIR}/src/strings/replace/replace_nulls.cu"
    "${CUDF_DIR}/src/strings/replace/replace_re.cu"
    "${CUDF_DIR}/src/strings/replace/replace_slice.cu"
    "${CUDF_DIR}/src/strings/reverse.cu"
    "${CUDF_DIR}/src/strings/scan/scan_inclusive.cu"
    "${CUDF_DIR}/src/strings/search/contains_multiple.cu"
    "${CUDF_DIR}/src/strings/search/count.cu"
    "${CUDF_DIR}/src/strings/search/findall.cu"
    "${CUDF_DIR}/src/strings/search/find.cu"
    "${CUDF_DIR}/src/strings/search/find_instance.cu"
    "${CUDF_DIR}/src/strings/search/find_multiple.cu"
    "${CUDF_DIR}/src/strings/slice.cu"
    "${CUDF_DIR}/src/strings/split/partition.cu"
    "${CUDF_DIR}/src/strings/split/split.cu"
    "${CUDF_DIR}/src/strings/split/split_part.cu"
    "${CUDF_DIR}/src/strings/split/split_re.cu"
    "${CUDF_DIR}/src/strings/split/split_record.cu"
    "${CUDF_DIR}/src/strings/strings_column_factories.cu"
    "${CUDF_DIR}/src/strings/strings_column_view.cpp"
    "${CUDF_DIR}/src/strings/strings_scalar_factories.cpp"
    "${CUDF_DIR}/src/strings/strip.cu"
    "${CUDF_DIR}/src/strings/translate.cu"
    "${CUDF_DIR}/src/strings/utilities.cu"
    "${CUDF_DIR}/src/strings/wrap.cu"
    "${CUDF_DIR}/src/structs/copying/concatenate.cu"
    "${CUDF_DIR}/src/structs/scan/scan_inclusive.cu"
    "${CUDF_DIR}/src/structs/structs_column_factories.cu"
    "${CUDF_DIR}/src/structs/structs_column_view.cpp"
    "${CUDF_DIR}/src/structs/utilities.cu"
    "${CUDF_DIR}/src/table/table.cpp"
    "${CUDF_DIR}/src/table/table_device_view.cu"
    "${CUDF_DIR}/src/table/table_equal.cu"
    "${CUDF_DIR}/src/table/table_view.cpp"
    "${CUDF_DIR}/src/text/deduplicate.cu"
    "${CUDF_DIR}/src/text/detokenize.cu"
    "${CUDF_DIR}/src/text/edit_distance.cu"
    "${CUDF_DIR}/src/text/generate_ngrams.cu"
    "${CUDF_DIR}/src/text/jaccard.cu"
    "${CUDF_DIR}/src/text/minhash.cu"
    "${CUDF_DIR}/src/text/ngrams_tokenize.cu"
    "${CUDF_DIR}/src/text/normalize.cu"
    "${CUDF_DIR}/src/text/replace.cu"
    "${CUDF_DIR}/src/text/stemmer.cu"
    "${CUDF_DIR}/src/text/bpe/byte_pair_encoding.cu"
    "${CUDF_DIR}/src/text/bpe/load_merge_pairs.cu"
    "${CUDF_DIR}/src/text/tokenize.cu"
    "${CUDF_DIR}/src/text/vocabulary_tokenize.cu"
    "${CUDF_DIR}/src/text/wordpiece_tokenize.cu"
    "${CUDF_DIR}/src/transform/bools_to_mask.cu"
    "${CUDF_DIR}/src/transform/compute_column.cu"
    "${CUDF_DIR}/src/transform/compute_column_kernel_complex.cu"
    "${CUDF_DIR}/src/transform/compute_column_kernel_null_complex.cu"
    "${CUDF_DIR}/src/transform/compute_column_kernel_null_primitive.cu"
    "${CUDF_DIR}/src/transform/compute_column_kernel_primitive.cu"
    "${CUDF_DIR}/src/transform/encode.cu"
    "${CUDF_DIR}/src/transform/mask_to_bools.cu"
    "${CUDF_DIR}/src/transform/nans_to_nulls.cu"
    "${CUDF_DIR}/src/transform/one_hot_encode.cu"
    "${CUDF_DIR}/src/transform/row_bit_count.cu"
    "${CUDF_DIR}/src/transform/transform.cu"
    "${CUDF_DIR}/src/transpose/transpose.cu"
    "${CUDF_DIR}/src/unary/cast_ops.cu"
    "${CUDF_DIR}/src/unary/math_ops.cu"
    "${CUDF_DIR}/src/unary/nan_ops.cu"
    "${CUDF_DIR}/src/unary/null_ops.cu"
    "${CUDF_DIR}/src/utilities/cuda.cpp"
    "${CUDF_DIR}/src/utilities/cuda_memcpy.cu"
    "${CUDF_DIR}/src/utilities/default_stream.cpp"
    "${CUDF_DIR}/src/utilities/getenv_or.cpp"
    "${CUDF_DIR}/src/utilities/grid_1d.cu"
    "${CUDF_DIR}/src/utilities/host_memory.cpp"
    "${CUDF_DIR}/src/utilities/host_worker_pool.cpp"
    "${CUDF_DIR}/src/utilities/linked_column.cpp"
    "${CUDF_DIR}/src/utilities/logger.cpp"
    "${CUDF_DIR}/src/utilities/prefetch.cpp"
    "${CUDF_DIR}/src/utilities/roaring_bitmap.cu"
    "${CUDF_DIR}/src/utilities/stream_pool.cpp"
    "${CUDF_DIR}/src/utilities/traits.cpp"
    "${CUDF_DIR}/src/utilities/type_checks.cpp"
    "${CUDF_DIR}/src/utilities/type_dispatcher.cpp"
    "${CUDF_EMBED_DIR}/cudf_cuda_embed.s"
    "${CUDF_EMBED_DIR}/cudf_fragments.s"
)

# Sources unreachable from src/GPU/GPUAggregationCudf.cpp: 212 of 469, 6.5 of cuDF's 16.6 CPU-hours.
# Derived, not hand-picked - every archive member outside the transitive closure of the island's
# undefined symbols. Regenerate by resolving `nm -u` of libclickhouse_gpu_cudf.a against
# `nm -A --defined-only` of lib_cudf.a. A new cuDF call fails the link naming what is missing.
list (REMOVE_ITEM CUDF_SOURCES
    "${CUDF_DIR}/src/ast/expression_parser.cpp"
    "${CUDF_DIR}/src/ast/operators.cpp"
    "${CUDF_DIR}/src/copying/contiguous_split.cu"
    "${CUDF_DIR}/src/copying/pack.cpp"
    "${CUDF_DIR}/src/copying/sample.cu"
    "${CUDF_DIR}/src/datetime/datetime_ops.cu"
    "${CUDF_DIR}/src/datetime/timezone.cpp"
    "${CUDF_DIR}/src/dictionary/decode.cu"
    "${CUDF_DIR}/src/dictionary/remove_keys.cu"
    "${CUDF_DIR}/src/filling/calendrical_month_sequence.cu"
    "${CUDF_DIR}/src/filling/repeat.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby.cpp"
    "${CUDF_DIR}/src/groupby/streaming_groupby/aggregate.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/impl.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/insert.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/insert_first.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/insert_first_nested.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/insert_nested.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/insert_subsequent.cu"
    "${CUDF_DIR}/src/groupby/streaming_groupby/insert_subsequent_nested.cu"
    "${CUDF_DIR}/src/hash/md5_hash.cu"
    "${CUDF_DIR}/src/hash/murmurhash3_x64_128.cu"
    "${CUDF_DIR}/src/hash/murmurhash3_x86_32.cu"
    "${CUDF_DIR}/src/hash/sha1_hash.cu"
    "${CUDF_DIR}/src/hash/sha224_hash.cu"
    "${CUDF_DIR}/src/hash/sha256_hash.cu"
    "${CUDF_DIR}/src/hash/sha384_hash.cu"
    "${CUDF_DIR}/src/hash/sha512_hash.cu"
    "${CUDF_DIR}/src/hash/xxhash_32.cu"
    "${CUDF_DIR}/src/hash/xxhash_64.cu"
    "${CUDF_DIR}/src/io/comp/brotli_dict.cpp"
    "${CUDF_DIR}/src/io/comp/common.cpp"
    "${CUDF_DIR}/src/io/comp/compression.cpp"
    "${CUDF_DIR}/src/io/comp/compression.cu"
    "${CUDF_DIR}/src/io/comp/cpu_unbz2.cpp"
    "${CUDF_DIR}/src/io/comp/debrotli.cu"
    "${CUDF_DIR}/src/io/comp/decompression.cpp"
    "${CUDF_DIR}/src/io/comp/gpuinflate.cu"
    "${CUDF_DIR}/src/io/comp/snap.cu"
    "${CUDF_DIR}/src/io/comp/unsnap.cu"
    "${CUDF_DIR}/src/io/utilities/base64_utilities.cpp"
    "${CUDF_DIR}/src/io/utilities/column_buffer.cpp"
    "${CUDF_DIR}/src/io/utilities/column_buffer_strings.cu"
    "${CUDF_DIR}/src/io/utilities/data_casting.cu"
    "${CUDF_DIR}/src/io/utilities/data_sink.cpp"
    "${CUDF_DIR}/src/io/utilities/datasource.cpp"
    "${CUDF_DIR}/src/io/utilities/row_selection.cpp"
    "${CUDF_DIR}/src/io/utilities/trie.cu"
    "${CUDF_DIR}/src/io/utilities/type_inference.cu"
    "${CUDF_DIR}/src/jit/row_ir.cpp"
    "${CUDF_DIR}/src/join/conditional_join.cu"
    "${CUDF_DIR}/src/join/cross_join.cu"
    "${CUDF_DIR}/src/join/distinct_hash_join.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_jit.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_kernel_complex.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_kernel_null_complex.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_kernel_null_primitive.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_kernel_primitive.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_output_size_kernel_complex.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_output_size_kernel_null_complex.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_output_size_kernel_null_primitive.cu"
    "${CUDF_DIR}/src/join/filter_join_indices/filter_join_indices_output_size_kernel_primitive.cu"
    "${CUDF_DIR}/src/join/filtered_join.cu"
    "${CUDF_DIR}/src/join/hash_join/finalize_partitioned_full_join.cpp"
    "${CUDF_DIR}/src/join/hash_join/full_join_match_context.cpp"
    "${CUDF_DIR}/src/join/hash_join/full_join_retrieve.cu"
    "${CUDF_DIR}/src/join/hash_join/full_join_size.cu"
    "${CUDF_DIR}/src/join/hash_join/full_join_size_impl.cu"
    "${CUDF_DIR}/src/join/hash_join/hash_join.cu"
    "${CUDF_DIR}/src/join/hash_join/inner_join_match_context.cpp"
    "${CUDF_DIR}/src/join/hash_join/inner_join_retrieve.cu"
    "${CUDF_DIR}/src/join/hash_join/inner_join_size.cu"
    "${CUDF_DIR}/src/join/hash_join/left_join_match_context.cpp"
    "${CUDF_DIR}/src/join/hash_join/left_join_retrieve.cu"
    "${CUDF_DIR}/src/join/hash_join/left_join_size.cu"
    "${CUDF_DIR}/src/join/hash_join/match_context.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_count.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_count_outer.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_full_join.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_inner_join.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_join_retrieve.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_left_join.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_retrieve.cu"
    "${CUDF_DIR}/src/join/hash_join/partitioned_retrieve_outer.cu"
    "${CUDF_DIR}/src/join/join_utils.cu"
    "${CUDF_DIR}/src/join/key_remapping.cu"
    "${CUDF_DIR}/src/join/mark_join.cu"
    "${CUDF_DIR}/src/join/mixed_join.cu"
    "${CUDF_DIR}/src/join/mixed_join_kernels_semi.cu"
    "${CUDF_DIR}/src/join/mixed_join_semi.cu"
    "${CUDF_DIR}/src/join/sort_merge_join.cu"
    "${CUDF_DIR}/src/json/json_path.cu"
    "${CUDF_DIR}/src/labeling/label_bins.cu"
    "${CUDF_DIR}/src/lists/combine/concatenate_list_elements.cu"
    "${CUDF_DIR}/src/lists/combine/concatenate_rows.cu"
    "${CUDF_DIR}/src/lists/copying/segmented_gather.cu"
    "${CUDF_DIR}/src/lists/count_elements.cu"
    "${CUDF_DIR}/src/lists/explode.cu"
    "${CUDF_DIR}/src/lists/sequences.cu"
    "${CUDF_DIR}/src/lists/set_operations.cu"
    "${CUDF_DIR}/src/partitioning/partitioning.cu"
    "${CUDF_DIR}/src/partitioning/round_robin.cu"
    "${CUDF_DIR}/src/quantiles/quantiles.cu"
    "${CUDF_DIR}/src/reductions/approx_distinct_count.cu"
    "${CUDF_DIR}/src/reductions/unique_count.cu"
    "${CUDF_DIR}/src/reductions/unique_count_column.cu"
    "${CUDF_DIR}/src/replace/clamp.cu"
    "${CUDF_DIR}/src/replace/nans.cu"
    "${CUDF_DIR}/src/replace/nulls.cu"
    "${CUDF_DIR}/src/reshape/byte_cast.cu"
    "${CUDF_DIR}/src/reshape/table_to_array.cu"
    "${CUDF_DIR}/src/reshape/tile.cu"
    "${CUDF_DIR}/src/rolling/detail/optimized_unbounded_window.cpp"
    "${CUDF_DIR}/src/rolling/detail/rolling_collect_list.cu"
    "${CUDF_DIR}/src/rolling/detail/rolling_fixed_window.cu"
    "${CUDF_DIR}/src/rolling/detail/rolling_utils.cu"
    "${CUDF_DIR}/src/rolling/detail/rolling_variable_window.cu"
    "${CUDF_DIR}/src/rolling/grouped_rolling.cu"
    "${CUDF_DIR}/src/rolling/range_rolling.cu"
    "${CUDF_DIR}/src/rolling/range_window_bounds.cpp"
    "${CUDF_DIR}/src/rolling/rolling.cpp"
    "${CUDF_DIR}/src/round/round.cu"
    "${CUDF_DIR}/src/search/contains_scalar.cu"
    "${CUDF_DIR}/src/search/contains_table_impl_nested.cu"
    "${CUDF_DIR}/src/search/contains_table_impl_primitive.cu"
    "${CUDF_DIR}/src/sort/is_sorted.cu"
    "${CUDF_DIR}/src/sort/top_k.cu"
    "${CUDF_DIR}/src/stream_compaction/drop_nans.cu"
    "${CUDF_DIR}/src/stream_compaction/drop_nulls.cu"
    "${CUDF_DIR}/src/stream_compaction/filter/filter.cu"
    "${CUDF_DIR}/src/stream_compaction/unique.cu"
    "${CUDF_DIR}/src/strings/attributes.cu"
    "${CUDF_DIR}/src/strings/capitalize.cu"
    "${CUDF_DIR}/src/strings/case.cu"
    "${CUDF_DIR}/src/strings/char_types/char_cases.cu"
    "${CUDF_DIR}/src/strings/char_types/char_types.cu"
    "${CUDF_DIR}/src/strings/combine/join_list_elements.cu"
    "${CUDF_DIR}/src/strings/convert/convert_booleans.cu"
    "${CUDF_DIR}/src/strings/convert/convert_datetime.cu"
    "${CUDF_DIR}/src/strings/convert/convert_durations.cu"
    "${CUDF_DIR}/src/strings/convert/convert_fixed_point.cu"
    "${CUDF_DIR}/src/strings/convert/convert_floats.cu"
    "${CUDF_DIR}/src/strings/convert/convert_hex.cu"
    "${CUDF_DIR}/src/strings/convert/convert_integers.cu"
    "${CUDF_DIR}/src/strings/convert/convert_ipv4.cu"
    "${CUDF_DIR}/src/strings/convert/convert_lists.cu"
    "${CUDF_DIR}/src/strings/convert/convert_urls.cu"
    "${CUDF_DIR}/src/strings/convert/int_cast.cu"
    "${CUDF_DIR}/src/strings/count_matches.cu"
    "${CUDF_DIR}/src/strings/extract/extract_all.cu"
    "${CUDF_DIR}/src/strings/filter_chars.cu"
    "${CUDF_DIR}/src/strings/like.cu"
    "${CUDF_DIR}/src/strings/padding.cu"
    "${CUDF_DIR}/src/strings/positions.cu"
    "${CUDF_DIR}/src/strings/regex/gkexec.cpp"
    "${CUDF_DIR}/src/strings/regex/glushkov_regcomp.cpp"
    "${CUDF_DIR}/src/strings/regex/regcomp.cpp"
    "${CUDF_DIR}/src/strings/regex/regex_program.cpp"
    "${CUDF_DIR}/src/strings/regex/regexec.cpp"
    "${CUDF_DIR}/src/strings/repeat_strings.cu"
    "${CUDF_DIR}/src/strings/replace/backref_re.cu"
    "${CUDF_DIR}/src/strings/replace/find_replace.cu"
    "${CUDF_DIR}/src/strings/replace/multi.cu"
    "${CUDF_DIR}/src/strings/replace/replace_nulls.cu"
    "${CUDF_DIR}/src/strings/replace/replace_re.cu"
    "${CUDF_DIR}/src/strings/replace/replace_slice.cu"
    "${CUDF_DIR}/src/strings/search/contains_multiple.cu"
    "${CUDF_DIR}/src/strings/search/count.cu"
    "${CUDF_DIR}/src/strings/search/find.cu"
    "${CUDF_DIR}/src/strings/search/find_instance.cu"
    "${CUDF_DIR}/src/strings/search/find_multiple.cu"
    "${CUDF_DIR}/src/strings/search/findall.cu"
    "${CUDF_DIR}/src/strings/split/partition.cu"
    "${CUDF_DIR}/src/strings/split/split.cu"
    "${CUDF_DIR}/src/strings/split/split_part.cu"
    "${CUDF_DIR}/src/strings/split/split_re.cu"
    "${CUDF_DIR}/src/strings/split/split_record.cu"
    "${CUDF_DIR}/src/strings/strings_scalar_factories.cpp"
    "${CUDF_DIR}/src/strings/strip.cu"
    "${CUDF_DIR}/src/strings/translate.cu"
    "${CUDF_DIR}/src/strings/wrap.cu"
    "${CUDF_DIR}/src/table/table_equal.cu"
    "${CUDF_DIR}/src/text/bpe/byte_pair_encoding.cu"
    "${CUDF_DIR}/src/text/bpe/load_merge_pairs.cu"
    "${CUDF_DIR}/src/text/deduplicate.cu"
    "${CUDF_DIR}/src/text/detokenize.cu"
    "${CUDF_DIR}/src/text/edit_distance.cu"
    "${CUDF_DIR}/src/text/generate_ngrams.cu"
    "${CUDF_DIR}/src/text/jaccard.cu"
    "${CUDF_DIR}/src/text/minhash.cu"
    "${CUDF_DIR}/src/text/ngrams_tokenize.cu"
    "${CUDF_DIR}/src/text/normalize.cu"
    "${CUDF_DIR}/src/text/stemmer.cu"
    "${CUDF_DIR}/src/text/tokenize.cu"
    "${CUDF_DIR}/src/text/vocabulary_tokenize.cu"
    "${CUDF_DIR}/src/text/wordpiece_tokenize.cu"
    "${CUDF_DIR}/src/transform/bools_to_mask.cu"
    "${CUDF_DIR}/src/transform/compute_column.cu"
    "${CUDF_DIR}/src/transform/compute_column_kernel_complex.cu"
    "${CUDF_DIR}/src/transform/compute_column_kernel_null_complex.cu"
    "${CUDF_DIR}/src/transform/compute_column_kernel_null_primitive.cu"
    "${CUDF_DIR}/src/transform/compute_column_kernel_primitive.cu"
    "${CUDF_DIR}/src/transform/mask_to_bools.cu"
    "${CUDF_DIR}/src/transform/nans_to_nulls.cu"
    "${CUDF_DIR}/src/transform/one_hot_encode.cu"
    "${CUDF_DIR}/src/transform/row_bit_count.cu"
    "${CUDF_DIR}/src/transform/transform.cu"
    "${CUDF_DIR}/src/transpose/transpose.cu"
    "${CUDF_DIR}/src/unary/nan_ops.cu"
    "${CUDF_DIR}/src/utilities/host_worker_pool.cpp"
    "${CUDF_DIR}/src/utilities/roaring_bitmap.cu"
)

add_library (_cudf ${CUDF_SOURCES})

gpu_island_target (_cudf)
add_dependencies (_cudf cudf_cuda_embed cudf_fragments)

target_include_directories (_cudf SYSTEM PUBLIC
    "${CUDF_DIR}/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
)
target_include_directories (_cudf SYSTEM PRIVATE
    "${CUDF_DIR}/src"
    "${CUDF_EMBED_DIR}"
    "${ClickHouse_SOURCE_DIR}/contrib/zstd/lib"
    # cuDF includes rtcx's umbrella header flat as `<rtcx.hpp>`; contrib/librtcx moved it under
    # `rtcx/`, so both directories are needed.
    "${ClickHouse_SOURCE_DIR}/contrib/librtcx/include/rtcx"
    "${GPU_CUDA_ROOT}/include"
)

# Restricted to what nvcc compiles: the target also holds two assembly blobs, and clang rejects
# `--expt-extended-lambda`. The force-include is because `cudf/detail/utilities/integer_utils.hpp`
# uses `CUDF_UNREACHABLE` under `__CUDA_ARCH__` without including its definition, which only
# matters here because every .cpp goes through nvcc and so reaches the device pass.
target_compile_options (_cudf PRIVATE
    "$<$<COMPILE_LANGUAGE:CUDA>:${CUDF_CUDA_FLAGS}>"
    "$<$<COMPILE_LANGUAGE:CUDA>:-include;cudf/detail/utilities/assert.cuh>")

# clang's integrated assembler resolves an `.incbin` against the include path rather than the
# source's own directory, and ignores `-isystem`.
target_compile_options (_cudf PRIVATE "$<$<COMPILE_LANGUAGE:ASM>:-I${CUDF_EMBED_DIR}>")

target_compile_definitions (_cudf PRIVATE
    CUDF_LOG_ACTIVE_LEVEL=RAPIDS_LOGGER_LOG_LEVEL_INFO
    # `ZSTD_findDecompressedSize` is declared only behind this; upstream sets it on its own zstd
    # target, where ClickHouse's contrib does not.
    ZSTD_STATIC_LINKING_ONLY
    # Both are upstream's, and both are workarounds it carries for the CCCL it pins.
    THRUST_FORCE_32_BIT_OFFSET_TYPE=1
    CCCL_AVOID_SORT_UNROLL=1
)

set_target_properties (_cudf PROPERTIES
    CUDA_STANDARD 20
    CUDA_STANDARD_REQUIRED ON
    CUDA_VISIBILITY_PRESET hidden
)

target_link_libraries (_cudf PUBLIC
    ch_contrib::cccl
    ch_contrib::rmm
    ch_contrib::rapids_logger
    ch_contrib::bs_thread_pool
    ch_contrib::dlpack
    ch_gpu::cudart
)

# The last three are ordinary ClickHouse targets whose INTERFACE_COMPILE_OPTIONS carry -Werror,
# which nvcc reads as taking a value - so they are linked with $<LINK_ONLY:> and their headers
# arrive through target_include_directories instead.
target_link_libraries (_cudf PRIVATE
    ch_contrib::nvtx3
    ch_contrib::cuco
    ch_contrib::kvikio
    ch_contrib::nvcomp
    ch_contrib::rtcx
    $<LINK_ONLY:ch_contrib::zlib>
    $<LINK_ONLY:ch_contrib::zstd>
    $<LINK_ONLY:ch_contrib::xxHash>
)
target_include_directories (_cudf SYSTEM PRIVATE
    "${ClickHouse_SOURCE_DIR}/contrib/zlib-ng"
    "${ClickHouse_SOURCE_DIR}/contrib/xxHash"
)

# Every fatbin the fragments produce lands in its own section, and the linker needs to be told
# to keep them together. Upstream writes the same script into its binary dir.
file (WRITE "${CMAKE_CURRENT_BINARY_DIR}/fatbin.ld"
"SECTIONS
{
  .nvFatBinSegment : { *(.nvFatBinSegment) }
  .nv_fatbin : { *(.nv_fatbin) }
}
")
target_link_options (_cudf PRIVATE "$<HOST_LINK:${CMAKE_CURRENT_BINARY_DIR}/fatbin.ld>")

add_library (ch_contrib::cudf ALIAS _cudf)
