add_library(divide_impl_sse2 divideImpl.cpp)
target_compile_options(divide_impl_sse2 PRIVATE -msse2 -DNAMESPACE=SSE2)
target_link_libraries(divide_impl_sse2 libdivide)

add_library(divide_impl_avx2 divideImpl.cpp)
target_compile_options(divide_impl_avx2 PRIVATE -mavx2 -DNAMESPACE=AVX2)
target_link_libraries(divide_impl_avx2 libdivide)

add_library(divide_impl divide.cpp)
target_link_libraries(divide_impl divide_impl_sse2 divide_impl_avx2 clickhouse_common_io)
