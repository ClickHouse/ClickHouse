set(EIGEN_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/eigen")

add_library (_eigen INTERFACE)

option (ENABLE_MKL "Build Eigen with Intel MKL" OFF)
if (ENABLE_MKL)
    set(MKL_THREADING sequential)
    set(MKL_INTERFACE lp64)
    find_package(MKL REQUIRED)
    if (MKL_FOUND)
        message("MKL INCLUDE: ${MKL_INCLUDE}")
        message("MKL LIBRARIES: ${MKL_LIBRARIES}")
        target_compile_definitions(_eigen INTERFACE EIGEN_USE_MKL_ALL)
        target_include_directories(_eigen INTERFACE ${MKL_INCLUDE})
        target_link_libraries(_eigen INTERFACE ${MKL_LIBRARIES})
    endif()
endif()

# Only include MPL2 code from Eigen library
target_compile_definitions(_eigen INTERFACE EIGEN_MPL2_ONLY)

target_include_directories (_eigen SYSTEM INTERFACE ${EIGEN_LIBRARY_DIR})
add_library(ch_contrib::eigen ALIAS _eigen)
