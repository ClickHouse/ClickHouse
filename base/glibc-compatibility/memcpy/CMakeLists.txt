if (ARCH_AMD64)
    add_library(memcpy STATIC memcpy.cpp)
    target_link_libraries(memcpy global-group)

    # We allow to include memcpy.h from user code for better inlining.
    target_include_directories(memcpy PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

    target_compile_options(memcpy PRIVATE -fno-builtin-memcpy)
endif ()
