set(USEARCH_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/usearch")

add_library(_usearch INTERFACE)
target_include_directories(_usearch SYSTEM INTERFACE ${USEARCH_PROJECT_DIR}/include)

target_link_libraries(_usearch INTERFACE _fp16)
target_compile_definitions(_usearch INTERFACE USEARCH_USE_FP16LIB)

# target_compile_definitions(_usearch INTERFACE USEARCH_USE_SIMSIMD)
# ^^ simsimd is not enabled at the moment. Reasons:
# - Vectorization is important for raw scans but not so much for HNSW. We use usearch only for HNSW.
# - Simsimd does compile-time dispatch (choice of SIMD kernels determined by capabilities of the build machine) or dynamic dispatch (SIMD
#   kernels chosen at runtime based on cpuid instruction). Since current builds are limited to SSE 4.2 (x86) and NEON (ARM), the speedup of
#   the former would be moderate compared to AVX-512 / SVE. The latter is at the moment too fragile with respect to portability across x86
#   and ARM machines ... certain conbinations of quantizations / distance functions / SIMD instructions are not implemented at the moment.

add_library(ch_contrib::usearch ALIAS _usearch)
