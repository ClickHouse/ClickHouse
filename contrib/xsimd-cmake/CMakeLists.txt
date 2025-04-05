set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/xsimd")
set(XSIMD_INCLUDE_DIR "${LIBRARY_DIR}/include")

add_library(xsimd INTERFACE)

target_include_directories(xsimd INTERFACE
        ${XSIMD_INCLUDE_DIR})
target_compile_features(xsimd INTERFACE cxx_std_11)
add_library(ch_contrib::xsimd ALIAS xsimd)