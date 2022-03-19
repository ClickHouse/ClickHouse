include(GNUInstallDirs)
set(CMAKE_CXX_STANDARD 11)

list(APPEND CMAKE_MODULE_PATH "${ClickHouse_SOURCE_DIR}/contrib/faiss/cmake")

option(FAISS_OPT_LEVEL "" "generic")

set(FAISS_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/faiss")
set(FAISS_SOURCE_DIR "${FAISS_PROJECT_DIR}/faiss")

set(FAISS_SRC
  ${FAISS_SOURCE_DIR}/AutoTune.cpp
  ${FAISS_SOURCE_DIR}/Clustering.cpp
  ${FAISS_SOURCE_DIR}/IVFlib.cpp
  ${FAISS_SOURCE_DIR}/Index.cpp
  ${FAISS_SOURCE_DIR}/Index2Layer.cpp
  ${FAISS_SOURCE_DIR}/IndexAdditiveQuantizer.cpp
  ${FAISS_SOURCE_DIR}/IndexBinary.cpp
  ${FAISS_SOURCE_DIR}/IndexBinaryFlat.cpp
  ${FAISS_SOURCE_DIR}/IndexBinaryFromFloat.cpp
  ${FAISS_SOURCE_DIR}/IndexBinaryHNSW.cpp
  ${FAISS_SOURCE_DIR}/IndexBinaryHash.cpp
  ${FAISS_SOURCE_DIR}/IndexBinaryIVF.cpp
  ${FAISS_SOURCE_DIR}/IndexFlat.cpp
  ${FAISS_SOURCE_DIR}/IndexFlatCodes.cpp
  ${FAISS_SOURCE_DIR}/IndexHNSW.cpp
  ${FAISS_SOURCE_DIR}/IndexIVF.cpp
  ${FAISS_SOURCE_DIR}/IndexIVFAdditiveQuantizer.cpp
  ${FAISS_SOURCE_DIR}/IndexIVFFlat.cpp
  ${FAISS_SOURCE_DIR}/IndexIVFPQ.cpp
  ${FAISS_SOURCE_DIR}/IndexIVFFastScan.cpp
  ${FAISS_SOURCE_DIR}/IndexIVFAdditiveQuantizerFastScan.cpp
  ${FAISS_SOURCE_DIR}/IndexIVFPQFastScan.cpp
  ${FAISS_SOURCE_DIR}/IndexIVFPQR.cpp
  ${FAISS_SOURCE_DIR}/IndexIVFSpectralHash.cpp
  ${FAISS_SOURCE_DIR}/IndexLSH.cpp
  ${FAISS_SOURCE_DIR}/IndexNNDescent.cpp
  ${FAISS_SOURCE_DIR}/IndexLattice.cpp
  ${FAISS_SOURCE_DIR}/IndexNSG.cpp
  ${FAISS_SOURCE_DIR}/IndexPQ.cpp
  ${FAISS_SOURCE_DIR}/IndexFastScan.cpp
  ${FAISS_SOURCE_DIR}/IndexAdditiveQuantizerFastScan.cpp
  ${FAISS_SOURCE_DIR}/IndexPQFastScan.cpp
  ${FAISS_SOURCE_DIR}/IndexPreTransform.cpp
  ${FAISS_SOURCE_DIR}/IndexRefine.cpp
  ${FAISS_SOURCE_DIR}/IndexReplicas.cpp
  ${FAISS_SOURCE_DIR}/IndexScalarQuantizer.cpp
  ${FAISS_SOURCE_DIR}/IndexShards.cpp
  ${FAISS_SOURCE_DIR}/MatrixStats.cpp
  ${FAISS_SOURCE_DIR}/MetaIndexes.cpp
  ${FAISS_SOURCE_DIR}/VectorTransform.cpp
  ${FAISS_SOURCE_DIR}/clone_index.cpp
  ${FAISS_SOURCE_DIR}/index_factory.cpp
  ${FAISS_SOURCE_DIR}/impl/AuxIndexStructures.cpp
  ${FAISS_SOURCE_DIR}/impl/FaissException.cpp
  ${FAISS_SOURCE_DIR}/impl/HNSW.cpp
  ${FAISS_SOURCE_DIR}/impl/NSG.cpp
  ${FAISS_SOURCE_DIR}/impl/PolysemousTraining.cpp
  ${FAISS_SOURCE_DIR}/impl/ProductQuantizer.cpp
  ${FAISS_SOURCE_DIR}/impl/AdditiveQuantizer.cpp
  ${FAISS_SOURCE_DIR}/impl/ResidualQuantizer.cpp
  ${FAISS_SOURCE_DIR}/impl/LocalSearchQuantizer.cpp
  ${FAISS_SOURCE_DIR}/impl/ScalarQuantizer.cpp
  ${FAISS_SOURCE_DIR}/impl/index_read.cpp
  ${FAISS_SOURCE_DIR}/impl/index_write.cpp
  ${FAISS_SOURCE_DIR}/impl/io.cpp
  ${FAISS_SOURCE_DIR}/impl/kmeans1d.cpp
  ${FAISS_SOURCE_DIR}/impl/lattice_Zn.cpp
  ${FAISS_SOURCE_DIR}/impl/pq4_fast_scan.cpp
  ${FAISS_SOURCE_DIR}/impl/pq4_fast_scan_search_1.cpp
  ${FAISS_SOURCE_DIR}/impl/pq4_fast_scan_search_qbs.cpp
  ${FAISS_SOURCE_DIR}/impl/io.cpp
  ${FAISS_SOURCE_DIR}/impl/lattice_Zn.cpp
  ${FAISS_SOURCE_DIR}/impl/NNDescent.cpp
  ${FAISS_SOURCE_DIR}/invlists/BlockInvertedLists.cpp
  ${FAISS_SOURCE_DIR}/invlists/DirectMap.cpp
  ${FAISS_SOURCE_DIR}/invlists/InvertedLists.cpp
  ${FAISS_SOURCE_DIR}/invlists/InvertedListsIOHook.cpp
  ${FAISS_SOURCE_DIR}/utils/Heap.cpp
  ${FAISS_SOURCE_DIR}/utils/WorkerThread.cpp
  ${FAISS_SOURCE_DIR}/utils/distances.cpp
  ${FAISS_SOURCE_DIR}/utils/distances_simd.cpp
  ${FAISS_SOURCE_DIR}/utils/extra_distances.cpp
  ${FAISS_SOURCE_DIR}/utils/hamming.cpp
  ${FAISS_SOURCE_DIR}/utils/partitioning.cpp
  ${FAISS_SOURCE_DIR}/utils/quantize_lut.cpp
  ${FAISS_SOURCE_DIR}/utils/random.cpp
  ${FAISS_SOURCE_DIR}/utils/utils.cpp
)

set(FAISS_HEADERS
  ${FAISS_SOURCE_DIR}/AutoTune.h
  ${FAISS_SOURCE_DIR}/Clustering.h
  ${FAISS_SOURCE_DIR}/IVFlib.h
  ${FAISS_SOURCE_DIR}/Index.h
  ${FAISS_SOURCE_DIR}/Index2Layer.h
  ${FAISS_SOURCE_DIR}/IndexAdditiveQuantizer.h
  ${FAISS_SOURCE_DIR}/IndexBinary.h
  ${FAISS_SOURCE_DIR}/IndexBinaryFlat.h
  ${FAISS_SOURCE_DIR}/IndexBinaryFromFloat.h
  ${FAISS_SOURCE_DIR}/IndexBinaryHNSW.h
  ${FAISS_SOURCE_DIR}/IndexBinaryHash.h
  ${FAISS_SOURCE_DIR}/IndexBinaryIVF.h
  ${FAISS_SOURCE_DIR}/IndexFlat.h
  ${FAISS_SOURCE_DIR}/IndexFlatCodes.h
  ${FAISS_SOURCE_DIR}/IndexHNSW.h
  ${FAISS_SOURCE_DIR}/IndexIVF.h
  ${FAISS_SOURCE_DIR}/IndexIVFAdditiveQuantizer.h
  ${FAISS_SOURCE_DIR}/IndexIVFFlat.h
  ${FAISS_SOURCE_DIR}/IndexIVFPQ.h
  ${FAISS_SOURCE_DIR}/IndexIVFFastScan.h
  ${FAISS_SOURCE_DIR}/IndexIVFAdditiveQuantizerFastScan.h
  ${FAISS_SOURCE_DIR}/IndexIVFPQFastScan.h
  ${FAISS_SOURCE_DIR}/IndexIVFPQR.h
  ${FAISS_SOURCE_DIR}/IndexIVFSpectralHash.h
  ${FAISS_SOURCE_DIR}/IndexLSH.h
  ${FAISS_SOURCE_DIR}/IndexLattice.h
  ${FAISS_SOURCE_DIR}/IndexNNDescent.h
  ${FAISS_SOURCE_DIR}/IndexNSG.h
  ${FAISS_SOURCE_DIR}/IndexPQ.h
  ${FAISS_SOURCE_DIR}/IndexFastScan.h
  ${FAISS_SOURCE_DIR}/IndexAdditiveQuantizerFastScan.h
  ${FAISS_SOURCE_DIR}/IndexPQFastScan.h
  ${FAISS_SOURCE_DIR}/IndexPreTransform.h
  ${FAISS_SOURCE_DIR}/IndexRefine.h
  ${FAISS_SOURCE_DIR}/IndexReplicas.h
  ${FAISS_SOURCE_DIR}/IndexScalarQuantizer.h
  ${FAISS_SOURCE_DIR}/IndexShards.h
  ${FAISS_SOURCE_DIR}/MatrixStats.h
  ${FAISS_SOURCE_DIR}/MetaIndexes.h
  ${FAISS_SOURCE_DIR}/MetricType.h
  ${FAISS_SOURCE_DIR}/VectorTransform.h
  ${FAISS_SOURCE_DIR}/clone_index.h
  ${FAISS_SOURCE_DIR}/index_factory.h
  ${FAISS_SOURCE_DIR}/index_io.h
  ${FAISS_SOURCE_DIR}/impl/AdditiveQuantizer.h
  ${FAISS_SOURCE_DIR}/impl/AuxIndexStructures.h
  ${FAISS_SOURCE_DIR}/impl/FaissAssert.h
  ${FAISS_SOURCE_DIR}/impl/FaissException.h
  ${FAISS_SOURCE_DIR}/impl/HNSW.h
  ${FAISS_SOURCE_DIR}/impl/LocalSearchQuantizer.h
  ${FAISS_SOURCE_DIR}/impl/LookupTableScaler.h
  ${FAISS_SOURCE_DIR}/impl/NNDescent.h
  ${FAISS_SOURCE_DIR}/impl/NSG.h
  ${FAISS_SOURCE_DIR}/impl/PolysemousTraining.h
  ${FAISS_SOURCE_DIR}/impl/ProductQuantizer-inl.h
  ${FAISS_SOURCE_DIR}/impl/ProductQuantizer.h
  ${FAISS_SOURCE_DIR}/impl/ResidualQuantizer.h
  ${FAISS_SOURCE_DIR}/impl/ResultHandler.h
  ${FAISS_SOURCE_DIR}/impl/ScalarQuantizer.h
  ${FAISS_SOURCE_DIR}/impl/ThreadedIndex-inl.h
  ${FAISS_SOURCE_DIR}/impl/ThreadedIndex.h
  ${FAISS_SOURCE_DIR}/impl/io.h
  ${FAISS_SOURCE_DIR}/impl/io_macros.h
  ${FAISS_SOURCE_DIR}/impl/kmeans1d.h
  ${FAISS_SOURCE_DIR}/impl/lattice_Zn.h
  ${FAISS_SOURCE_DIR}/impl/platform_macros.h
  ${FAISS_SOURCE_DIR}/impl/pq4_fast_scan.h
  ${FAISS_SOURCE_DIR}/impl/simd_result_handlers.h
  ${FAISS_SOURCE_DIR}/invlists/BlockInvertedLists.h
  ${FAISS_SOURCE_DIR}/invlists/DirectMap.h
  ${FAISS_SOURCE_DIR}/invlists/InvertedLists.h
  ${FAISS_SOURCE_DIR}/invlists/InvertedListsIOHook.h
  ${FAISS_SOURCE_DIR}/utils/AlignedTable.h
  ${FAISS_SOURCE_DIR}/utils/Heap.h
  ${FAISS_SOURCE_DIR}/utils/WorkerThread.h
  ${FAISS_SOURCE_DIR}/utils/distances.h
  ${FAISS_SOURCE_DIR}/utils/extra_distances-inl.h
  ${FAISS_SOURCE_DIR}/utils/extra_distances.h
  ${FAISS_SOURCE_DIR}/utils/hamming-inl.h
  ${FAISS_SOURCE_DIR}/utils/hamming.h
  ${FAISS_SOURCE_DIR}/utils/ordered_key_value.h
  ${FAISS_SOURCE_DIR}/utils/partitioning.h
  ${FAISS_SOURCE_DIR}/utils/quantize_lut.h
  ${FAISS_SOURCE_DIR}/utils/random.h
  ${FAISS_SOURCE_DIR}/utils/simdlib.h
  ${FAISS_SOURCE_DIR}/utils/simdlib_avx2.h
  ${FAISS_SOURCE_DIR}/utils/simdlib_emulated.h
  ${FAISS_SOURCE_DIR}/utils/simdlib_neon.h
  ${FAISS_SOURCE_DIR}/utils/utils.h
)

if(NOT WIN32)
  list(APPEND FAISS_SRC ${FAISS_SOURCE_DIR}/invlists/OnDiskInvertedLists.cpp)
  list(APPEND FAISS_HEADERS ${FAISS_SOURCE_DIR}/invlists/OnDiskInvertedLists.h)
endif()

# Export FAISS_HEADERS variable to parent scope.
set(FAISS_HEADERS ${FAISS_HEADERS} PARENT_SCOPE)

add_library(_faiss ${FAISS_SRC})

add_library(_faiss_avx2 ${FAISS_SRC})
if(NOT FAISS_OPT_LEVEL STREQUAL "avx2")
  set_target_properties(_faiss_avx2 PROPERTIES EXCLUDE_FROM_ALL TRUE)
endif()
if(NOT WIN32)
  target_compile_options(_faiss_avx2 PRIVATE $<$<COMPILE_LANGUAGE:CXX>:-mavx2 -mfma -mf16c -mpopcnt>)
else()
  # MSVC enables FMA with /arch:AVX2; no separate flags for F16C, POPCNT
  # Ref. FMA (under /arch:AVX2): https://docs.microsoft.com/en-us/cpp/build/reference/arch-x64
  # Ref. F16C (2nd paragraph): https://walbourn.github.io/directxmath-avx2/
  # Ref. POPCNT: https://docs.microsoft.com/en-us/cpp/intrinsics/popcnt16-popcnt-popcnt64
  target_compile_options(_faiss_avx2 PRIVATE $<$<COMPILE_LANGUAGE:CXX>:/arch:AVX2>)
endif()

# Handle `#include <faiss/foo.h>`.
target_include_directories(_faiss SYSTEM PUBLIC 
  $<BUILD_INTERFACE:${FAISS_PROJECT_DIR}>)
# Handle `#include <faiss/foo.h>`.
target_include_directories(_faiss_avx2 SYSTEM PUBLIC 
  $<BUILD_INTERFACE:${FAISS_PROJECT_DIR}>)

set_target_properties(_faiss PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  WINDOWS_EXPORT_ALL_SYMBOLS ON
)
set_target_properties(_faiss_avx2 PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  WINDOWS_EXPORT_ALL_SYMBOLS ON
)

if(WIN32)
  target_compile_definitions(_faiss PRIVATE FAISS_MAIN_LIB)
  target_compile_definitions(_faiss_avx2 PRIVATE FAISS_MAIN_LIB)
endif()

target_compile_definitions(_faiss PRIVATE FINTEGER=int)
target_compile_definitions(_faiss_avx2 PRIVATE FINTEGER=int)

find_package(OpenMP REQUIRED)
target_link_libraries(_faiss PRIVATE OpenMP::OpenMP_CXX)
target_link_libraries(_faiss PRIVATE OpenMP::OpenMP_C)
target_link_libraries(_faiss_avx2 PRIVATE OpenMP::OpenMP_CXX)
message("CXX dirs: ${OpenMP_CXX_INCLUDE_DIRS}")
message("C dirs: ${OpenMP_C_INCLUDE_DIRS}")
message("OpenMP_C_FLAGS = ${OpenMP_C_FLAGS}")
message("OpenMP_CXX_FLAGS = ${OpenMP_CXX_FLAGS}")
message("OpenMP_C_INCLUDE_DIRS = ${OpenMP_C_INCLUDE_DIRS}")
message("OpenMP_CXX_INCLUDE_DIRS = ${OpenMP_CXX_INCLUDE_DIRS}")
message("OpenMP_C_LIBRARIES = ${OpenMP_C_LIBRARIES}")
message("OpenMP_CXX_LIBRARIES = ${OpenMP_CXX_LIBRARIES}")
target_include_directories(_faiss PRIVATE "/usr/include/clang/13/include")
target_include_directories(_faiss_avx2 PRIVATE "/usr/include/clang/13/include")

find_package(MKL)
if(MKL_FOUND)
  target_link_libraries(_faiss PRIVATE ${MKL_LIBRARIES})
  target_link_libraries(_faiss_avx2 PRIVATE ${MKL_LIBRARIES})
else()
  find_package(BLAS REQUIRED)
  target_link_libraries(_faiss PRIVATE ${BLAS_LIBRARIES})
  target_link_libraries(_faiss_avx2 PRIVATE ${BLAS_LIBRARIES})

  find_package(LAPACK REQUIRED)
  target_link_libraries(_faiss PRIVATE ${LAPACK_LIBRARIES})
  target_link_libraries(_faiss_avx2 PRIVATE ${LAPACK_LIBRARIES})
endif()

add_library(ch_contrib::faiss ALIAS _faiss)
