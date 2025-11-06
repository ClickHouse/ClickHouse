# https://llvm.org/docs/XRay.html

option (ENABLE_XRAY "Enable LLVM XRay" OFF)

if (NOT ENABLE_XRAY)
    message (STATUS "Not using LLVM XRay")
    return()
endif()

if (NOT ((ARCH_AMD64 OR ARCH_AARCH64) AND OS_LINUX))
    message (STATUS "Not using LLVM XRay, only Linux on amd64 and aarch64 is supported")
    return()
endif()

# The target clang must support xray, otherwise it should error on invalid option
set (XRAY_FLAGS "-fxray-instrument")
set (USE_XRAY 1)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${XRAY_FLAGS}")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${XRAY_FLAGS}")

message (STATUS "Using LLVM XRay")
