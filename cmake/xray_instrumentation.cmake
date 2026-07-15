# https://llvm.org/docs/XRay.html

option (ENABLE_XRAY "Enable LLVM XRay" OFF)

if (NOT ENABLE_XRAY)
    message (STATUS "Not using LLVM XRay")
    return()
endif()

if (NOT (ARCH_AMD64 AND OS_LINUX))
    message (STATUS "Not using LLVM XRay, only amd64 Linux or FreeBSD are supported")
    return()
endif()

# The target clang must support xray, otherwise it should error on invalid option
set (XRAY_FLAGS "-fxray-instrument -DUSE_XRAY")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${XRAY_FLAGS}")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${XRAY_FLAGS}")

message (STATUS "Using LLVM XRay")
