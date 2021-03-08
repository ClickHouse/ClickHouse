enable_language(ASM)
add_library(memcpy STATIC memcpy.S)

# We allow to include memcpy.h from user code for better inlining (less number of registers are clobbered?).
target_include_directories(memcpy PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
