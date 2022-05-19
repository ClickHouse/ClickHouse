set(EIGEN_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/eigen")

add_library (_eigen INTERFACE)

# Only include MPL2 code from Eigen library
target_compile_definitions(_eigen INTERFACE EIGEN_MPL2_ONLY)

# Clang by default mimics gcc 4.2.1 compatibility but Eigen checks __GNUC__ version to enable
# a workaround for bug https://gcc.gnu.org/bugzilla/show_bug.cgi?id=72867 fixed in 6.3
# So we fake gcc > 6.3 when building with clang
if (COMPILER_CLANG AND ARCH_PPC64LE)
    target_compile_options(_eigen INTERFACE -fgnuc-version=6.4)
endif()

target_include_directories (_eigen SYSTEM INTERFACE ${EIGEN_LIBRARY_DIR})
add_library(ch_contrib::eigen ALIAS _eigen)
