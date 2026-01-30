if (ARCH_AMD64 OR ARCH_AARCH64)
    add_library(memcpy STATIC memcpy.cpp)

    # We allow to include memcpy.h from user code for better inlining.
    target_include_directories(memcpy PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

    target_compile_options(memcpy PRIVATE -fno-builtin-memcpy)

    if (ARCH_AARCH64)
        target_link_libraries (memcpy PRIVATE ch_contrib::stringzilla)
    endif ()
endif ()
