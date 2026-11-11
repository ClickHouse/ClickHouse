# cuVS, the vector search library of RAPIDS, for the `cagra` method of the vector similarity index. On the GPU island,
# like cuDF (see gpu_island_target in cmake/cuda.cmake). Tests, benchmarks, the C API and the Python pieces are not built.
#
# Only what CAGRA needs is compiled: CAGRA build, search and serialization, IVF-PQ and NN-descent, which build its graph,
# IVF-Flat's scan and refine, which IVF-PQ uses, the VPQ compression of CAGRA-Q, balanced k-means, select-k and the
# pairwise distances. Upstream generates most of its translation units and all its JIT-LTO kernel fragments from JSON
# matrices. Those calls of its
# cpp/CMakeLists.txt are evaluated here as they are (see cuvs_evaluate_upstream below), so the instantiations stay exactly
# upstream's when the submodule moves; the list of sources of the library is written out here. cuTile kernels and the
# multi-GPU algorithms are off.

if (NOT ENABLE_GPU)
    message (STATUS "Not using cuVS")
    return ()
endif ()

set (CUVS_CPP_DIR "${ClickHouse_SOURCE_DIR}/contrib/cuvs/cpp")
set (CUVS_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

# What upstream's cmake/modules/ConfigureCUDA.cmake sets, without the -Werror it adds for gcc.
set (CUVS_CUDA_FLAGS
    --expt-extended-lambda
    --expt-relaxed-constexpr
    -DCUDA_API_PER_THREAD_DEFAULT_STREAM
    -DRAFT_HIDE_DEPRECATION_WARNINGS
    -Xcompiler=-Wno-deprecated-declarations
    -Xcompiler=-fopenmp)
set (CUVS_CXX_FLAGS -Wno-deprecated-declarations -DRAFT_HIDE_DEPRECATION_WARNINGS -DCUDA_API_PER_THREAD_DEFAULT_STREAM)

# The names upstream's generator calls link the kernels with.
add_library (rmm::rmm ALIAS _rmm)
add_library (raft::raft ALIAS _raft)
add_library (CCCL::CCCL ALIAS _cccl)
add_library (cuco::cuco ALIAS _cuco)

include ("${CUVS_CPP_DIR}/cmake/modules/generate_inst_matrix.cmake")
include ("${CUVS_CPP_DIR}/cmake/modules/generate_string_matrix.cmake")
include ("${CMAKE_CURRENT_SOURCE_DIR}/island_jit_lto.cmake")

set (BUILD_MG_ALGOS OFF)

# Evaluates the part of upstream's cpp/CMakeLists.txt from the line `BEGIN` up to, not including, the line `END`, with its
# source and binary directories replaced by the ones of this build.
file (READ "${CUVS_CPP_DIR}/CMakeLists.txt" CUVS_UPSTREAM_CMAKELISTS)
macro (cuvs_evaluate_upstream BEGIN END)
    string (FIND "${CUVS_UPSTREAM_CMAKELISTS}" "${BEGIN}" _cuvs_begin)
    string (FIND "${CUVS_UPSTREAM_CMAKELISTS}" "${END}" _cuvs_end)
    if (_cuvs_begin EQUAL -1 OR _cuvs_end EQUAL -1 OR NOT _cuvs_end GREATER _cuvs_begin)
        message (FATAL_ERROR "contrib/cuvs/cpp/CMakeLists.txt changed: cannot find the block from '${BEGIN}' to '${END}'")
    endif ()
    math (EXPR _cuvs_length "${_cuvs_end} - ${_cuvs_begin}")
    string (SUBSTRING "${CUVS_UPSTREAM_CMAKELISTS}" ${_cuvs_begin} ${_cuvs_length} _cuvs_code)
    string (REPLACE "\${CMAKE_CURRENT_SOURCE_DIR}" "\${CUVS_CPP_DIR}" _cuvs_code "${_cuvs_code}")
    string (REPLACE "\${CMAKE_CURRENT_BINARY_DIR}" "\${CUVS_GENERATED_DIR}" _cuvs_code "${_cuvs_code}")
    cmake_language (EVAL CODE "${_cuvs_code}")
endmacro ()

# The instantiations of CAGRA search, and the JIT-LTO kernel fragments of all of the library.
cuvs_evaluate_upstream (
    "  generate_inst_matrix(\n    cagra_search_inst_files"
    "  include(cmake/modules/generate_cutile_kernels.cmake)")
# The other instantiations.
cuvs_evaluate_upstream (
    "  generate_inst_matrix(\n    pairwise_matrix_dispatch_inst_files"
    "  add_library(\n    cuvs_objs OBJECT")

# ClickHouse forbids compile options in the interface of a contrib target (cmake/sanitize_targets.cmake); the kernels get
# them from add_jit_lto_kernel instead.
set_target_properties (jit_lto_kernel_usage_requirements PROPERTIES INTERFACE_COMPILE_OPTIONS "")

# The fragments of the indexes that are not built.
list (FILTER jit_lto_files EXCLUDE REGEX "/generated_kernels/(ivf_sq|ivf_rabitq)/")

set (CUVS_SOURCES
    src/neighbors/cagra.cpp
    src/neighbors/detail/cagra/graph_shared.cu
    src/neighbors/detail/cagra/cagra_helpers.cpp
    src/neighbors/detail/cagra/topk_for_cagra/topk.cu
    src/neighbors/ivf_common.cu
    src/neighbors/ivf_pq_index.cu
    src/neighbors/ivf_pq/ivf_pq_build_common.cu
    src/neighbors/ivf_pq/detail/ivf_pq_build_precomputed_int64_t.cu
    src/neighbors/ivf_pq/detail/ivf_pq_contiguous_list_data.cu
    src/neighbors/ivf_pq/detail/ivf_pq_list_data.cu
    src/neighbors/ivf_pq/detail/ivf_pq_process_and_fill_codes.cu
    src/neighbors/ivf_flat_index.cpp
    src/neighbors/ivf_flat/ivf_flat_helpers.cu
    src/neighbors/nn_descent.cu
    src/neighbors/nn_descent_gnnd_inst.cu
    src/neighbors/nn_descent_index.cpp
    src/neighbors/sample_filter.cu
    src/neighbors/roaring_bitmap_filter.cu
    src/core/bitset.cu
    src/core/bloom_filter.cu
    src/core/roaring_allowlist.cu
    src/core/omp_wrapper.cpp
    src/util/file_io.cpp
    src/util/host_memory.cpp
    src/preprocessing/quantize/bbq.cu
    src/cluster/detail/minClusterDistanceCompute.cu
    src/cluster/kmeans_cluster_cost.cu
    src/cluster/kmeans_fit_float.cu
    src/cluster/kmeans_predict_float.cu
    src/cluster/kmeans_balanced_fit_float.cu
    src/cluster/kmeans_balanced_fit_half.cu
    src/cluster/kmeans_balanced_fit_int8.cu
    src/cluster/kmeans_balanced_fit_uint8.cu
    src/cluster/kmeans_balanced_fit_predict_float.cu
    src/cluster/kmeans_balanced_fit_predict_int8.cu
    src/cluster/kmeans_balanced_predict_float.cu
    src/cluster/kmeans_balanced_predict_half.cu
    src/cluster/kmeans_balanced_predict_int8.cu
    src/cluster/kmeans_balanced_predict_uint8.cu
    src/distance/distance.cu
    src/distance/pairwise_distance.cu
    src/distance/top_1_nn.cu)
list (TRANSFORM CUVS_SOURCES PREPEND "${CUVS_CPP_DIR}/")

list (APPEND CUVS_SOURCES
    # The VPQ compression of CAGRA-Q, instead of src/preprocessing/quantize/pq.cu: see the comment in it.
    "${CMAKE_CURRENT_SOURCE_DIR}/vpq_train_from_rows.cu"
    ${cagra_build_inst_files}
    ${cagra_serialize_inst_files}
    ${cuvs_cagra_search_cuda_inst_files}
    ${ivf_pq_build_extend_inst_files}
    ${ivf_pq_compute_similarity_inst_files}
    ${ivf_pq_search_inst_files}
    ${ivf_pq_transform_inst_files}
    ${ivf_flat_interleaved_scan_inst_files}
    ${nn_descent_inst_files}
    ${refine_device_inst_files}
    ${refine_host_inst_files}
    ${select_k_inst_files}
    ${pairwise_matrix_dispatch_inst_files}
    ${jit_lto_files})

add_library (_cuvs STATIC ${CUVS_SOURCES})

# First, before any of the properties below: this wipes the target clean.
gpu_island_target (_cuvs)

target_include_directories (_cuvs SYSTEM PUBLIC "${CUVS_CPP_DIR}/include")
target_include_directories (_cuvs SYSTEM PRIVATE
    "${CUVS_CPP_DIR}/src"
    # The generated neighbors/detail/cagra/compute_distance-ext.cuh.
    "${CUVS_GENERATED_DIR}/src"
    "${GPU_CUDA_ROOT}/include")

target_compile_definitions (_cuvs PRIVATE
    CUVS_CUTILE_ENABLED=0
    CUVS_CUTILE_SM107_ENABLED=0
    CUVS_SYSTEM_LITTLE_ENDIAN=1)
target_compile_options (_cuvs PRIVATE ${CUVS_CUDA_FLAGS})

set_target_properties (_cuvs PROPERTIES
    CUDA_STANDARD 20
    CUDA_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
    CUDA_VISIBILITY_PRESET hidden)

# The math libraries of CUDA and OpenMP statically, like the CUDA runtime: nothing of cuVS is needed at run time.
gpu_cuda_library (CUVS_CUBLAS_LIBRARY cublas_static)
gpu_cuda_library (CUVS_CUBLASLT_LIBRARY cublasLt_static)
gpu_cuda_library (CUVS_CUSOLVER_LIBRARY cusolver_static)
gpu_cuda_library (CUVS_CUSOLVER_LAPACK_LIBRARY cusolver_lapack_static)
gpu_cuda_library (CUVS_CUSOLVER_METIS_LIBRARY cusolver_metis_static)
gpu_cuda_library (CUVS_METIS_LIBRARY metis_static)
gpu_cuda_library (CUVS_CUSPARSE_LIBRARY cusparse_static)
gpu_cuda_library (CUVS_CURAND_LIBRARY curand_static)

execute_process (
    COMMAND "${CMAKE_CUDA_HOST_COMPILER}" -print-file-name=libgomp.a
    OUTPUT_VARIABLE CUVS_GOMP_LIBRARY
    OUTPUT_STRIP_TRAILING_WHITESPACE
    COMMAND_ERROR_IS_FATAL ANY)
if (NOT IS_ABSOLUTE "${CUVS_GOMP_LIBRARY}" OR NOT EXISTS "${CUVS_GOMP_LIBRARY}")
    message (FATAL_ERROR "${CMAKE_CUDA_HOST_COMPILER} has no libgomp.a (got '${CUVS_GOMP_LIBRARY}').")
endif ()

target_link_libraries (_cuvs PUBLIC
    ch_contrib::raft
    ch_contrib::rmm
    ch_contrib::cccl
    ch_contrib::rapids_logger
    ch_gpu::cudart)
target_link_libraries (_cuvs PRIVATE
    ch_contrib::cuco
    ch_contrib::cutlass
    ch_contrib::kvikio
    ch_contrib::rtcx
    ch_contrib::nvtx3
    "${CUVS_CUSOLVER_LIBRARY}"
    "${CUVS_CUSOLVER_LAPACK_LIBRARY}"
    "${CUVS_CUSOLVER_METIS_LIBRARY}"
    "${CUVS_METIS_LIBRARY}"
    "${CUVS_CUSPARSE_LIBRARY}"
    "${CUVS_CURAND_LIBRARY}"
    "${CUVS_CUBLAS_LIBRARY}"
    "${CUVS_CUBLASLT_LIBRARY}"
    "${CUVS_GOMP_LIBRARY}")

add_library (ch_contrib::cuvs ALIAS _cuvs)
