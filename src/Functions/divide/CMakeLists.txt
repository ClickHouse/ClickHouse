# A library for integer division by constant with CPU dispatching.

if (ARCH_AMD64)
    add_library(divide_impl_sse2 divideImpl.cpp)
    target_compile_options(divide_impl_sse2 PRIVATE -msse2 -DNAMESPACE=SSE2)
    target_link_libraries(divide_impl_sse2 libdivide)

    add_library(divide_impl_avx2 divideImpl.cpp)
    target_compile_options(divide_impl_avx2 PRIVATE -mavx2 -DNAMESPACE=AVX2)
    target_link_libraries(divide_impl_avx2 libdivide)

    set(IMPLEMENTATIONS divide_impl_sse2 divide_impl_avx2)
else ()
    add_library(divide_impl_generic divideImpl.cpp)
    target_compile_options(divide_impl_generic PRIVATE -DNAMESPACE=Generic)
    target_link_libraries(divide_impl_generic libdivide)

    set(IMPLEMENTATIONS divide_impl_generic)
endif ()

add_library(divide_impl divide.cpp)
target_link_libraries(divide_impl ${IMPLEMENTATIONS} clickhouse_common_io)
