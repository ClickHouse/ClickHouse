if (ARCH_AMD64 OR ARCH_AARCH64)
    add_library(memcmp STATIC memcmp.cpp)

    # We allow to include memcmp.h from user code for better inlining.
    target_include_directories(memcmp PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

    target_compile_options(memcmp PRIVATE -fno-builtin-memcmp)
endif ()
