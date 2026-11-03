set(USEARCH_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/usearch")

add_library(_usearch INTERFACE)
target_include_directories(_usearch SYSTEM INTERFACE ${USEARCH_PROJECT_DIR}/include)

if (TARGET _numkong)
    target_link_libraries(_usearch INTERFACE _numkong)
    target_compile_definitions(_usearch INTERFACE USEARCH_USE_NUMKONG=1)

    target_compile_definitions(_usearch INTERFACE USEARCH_CAN_COMPILE_FP16=1)
    # usearch only uses this to pick `NK_NATIVE_BF16`. Native `__bf16` needs AVX-512 BF16 or ARM BF16 enabled for the
    # whole build, and `c/numkong.c` compiles its entry points with `NK_NATIVE_BF16=0` anyway.
    target_compile_definitions(_usearch INTERFACE USEARCH_CAN_COMPILE_BF16=0)
endif ()

add_library(ch_contrib::usearch ALIAS _usearch)
