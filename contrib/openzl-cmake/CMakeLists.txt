# OpenZL - Format-aware compression framework from Meta
# https://github.com/facebook/openzl
#
# This CMake wrapper builds OpenZL directly using ClickHouse's existing
# zstd and lz4 libraries instead of OpenZL's bundled dependencies.

set(OPENZL_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/openzl")
set(OPENZL_SRC_DIR "${OPENZL_SOURCE_DIR}/src")
set(OPENZL_INCLUDE_DIR "${OPENZL_SOURCE_DIR}/include")

if(NOT EXISTS "${OPENZL_SOURCE_DIR}/CMakeLists.txt")
    message(WARNING "OpenZL submodule not found. Run: git submodule update --init contrib/openzl")
    return()
endif()

# Collect all C source files from src/openzl
file(GLOB_RECURSE OPENZL_SOURCES CONFIGURE_DEPENDS "${OPENZL_SRC_DIR}/*.c")
file(GLOB_RECURSE OPENZL_HEADERS CONFIGURE_DEPENDS "${OPENZL_SRC_DIR}/*.h")
file(GLOB_RECURSE OPENZL_PUBLIC_HEADERS CONFIGURE_DEPENDS "${OPENZL_INCLUDE_DIR}/*.h")

# Generate zl_config.h
# Check for variadic macro support
include(CheckCSourceCompiles)
check_c_source_compiles("
    #define F(...) f(__VA_ARGS__)
    void f(int);
    int main() { F(0); return 0; }
" ZL_HAVE_C_VARIADIC_MACROS)

check_c_source_compiles("
    #define F(a, ...) f(a, ##__VA_ARGS__)
    void f(int, ...);
    int main() { F(0); return 0; }
" ZL_HAVE_C_GNU_ZERO_VARIADIC_MACRO_ARGUMENTS)

# Determine architecture
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
    set(ZL_HAVE_X86_64_ASM 1)
else()
    set(ZL_HAVE_X86_64_ASM 0)
endif()

# Disable introspection for production builds
set(ZL_ALLOW_INTROSPECTION 0)

# Configure zl_config.h
set(ZL_CONFIG_H "${CMAKE_CURRENT_BINARY_DIR}/include/openzl/zl_config.h")
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include/openzl")
configure_file(
    "${OPENZL_SOURCE_DIR}/build-scripts/cmake/zl_config.h.cmake"
    "${ZL_CONFIG_H}"
)

# Add x86_64 assembly if available
if(ZL_HAVE_X86_64_ASM)
    set(OPENZL_ASM_SOURCES "${OPENZL_SRC_DIR}/openzl/fse/decompress/huf_decompress_amd64.S")
    if(EXISTS "${OPENZL_ASM_SOURCES}")
        set_property(
            SOURCE "${OPENZL_ASM_SOURCES}"
            APPEND PROPERTY COMPILE_OPTIONS "-x" "assembler-with-cpp"
        )
        list(APPEND OPENZL_SOURCES "${OPENZL_ASM_SOURCES}")
    endif()
endif()

# Create the openzl library
add_library(_openzl ${OPENZL_SOURCES} ${OPENZL_HEADERS} "${ZL_CONFIG_H}")

# Use ClickHouse's zstd and lz4 instead of bundled ones
target_link_libraries(_openzl
    PUBLIC
        ch_contrib::zstd
        ch_contrib::lz4
)

# Find math library
find_library(MATH_LIBRARY m)
if(MATH_LIBRARY)
    target_link_libraries(_openzl PUBLIC ${MATH_LIBRARY})
endif()

# Find threads
set(CMAKE_THREAD_PREFER_PTHREAD ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(_openzl PUBLIC Threads::Threads)

# Include directories
target_include_directories(_openzl
    PUBLIC
        "${OPENZL_INCLUDE_DIR}"
        "${CMAKE_CURRENT_BINARY_DIR}/include"
    PRIVATE
        "${OPENZL_SOURCE_DIR}"
        "${OPENZL_SRC_DIR}"
)

# Compiler settings
set_property(TARGET _openzl PROPERTY C_STANDARD 11)
set_property(TARGET _openzl PROPERTY C_STANDARD_REQUIRED ON)
set_property(TARGET _openzl PROPERTY POSITION_INDEPENDENT_CODE ON)

# Disable introspection by default for production builds
target_compile_definitions(_openzl PRIVATE ZL_ALLOW_INTROSPECTION=0)

# Suppress warnings from third-party code
target_compile_options(_openzl PRIVATE -w)

# Create alias for ClickHouse integration
add_library(ch_contrib::openzl ALIAS _openzl)
