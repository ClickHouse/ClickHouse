set(EIGEN_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/eigen")

add_library (_eigen INTERFACE)

# Only include MPL2 code from Eigen library
target_compile_definitions(_eigen INTERFACE EIGEN_MPL2_ONLY)

target_include_directories (_eigen SYSTEM INTERFACE ${EIGEN_LIBRARY_DIR})
add_library(ch_contrib::eigen ALIAS _eigen)
