set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/fastops)

set(SRCS "")

set (SRCS ${SRCS} ${LIBRARY_DIR}/fastops/avx/ops_avx.cpp)
set_source_files_properties(${LIBRARY_DIR}/fastops/avx/ops_avx.cpp PROPERTIES COMPILE_FLAGS "-mavx -DNO_AVX2")

set (SRCS ${SRCS} ${LIBRARY_DIR}/fastops/avx2/ops_avx2.cpp)
set_source_files_properties(${LIBRARY_DIR}/fastops/avx2/ops_avx2.cpp PROPERTIES COMPILE_FLAGS "-mavx2 -mfma")

set (SRCS ${SRCS} ${LIBRARY_DIR}/fastops/plain/ops_plain.cpp ${LIBRARY_DIR}/fastops/core/avx_id.cpp ${LIBRARY_DIR}/fastops/fastops.cpp)

add_library(fastops ${SRCS})

target_include_directories(fastops SYSTEM PUBLIC "${LIBRARY_DIR}")
