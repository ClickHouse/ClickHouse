# https://llvm.org/docs/XRay.html

if (ARCH_AMD64)
    option (ENABLE_XRAY "Enable LLVM XRay" ON)
else ()
    option (ENABLE_XRAY "Enable LLVM XRay" OFF)
endif ()

set (XRAY_FLAGS "-fxray-instrument -DUSE_XRAY")

if (ENABLE_XRAY)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${XRAY_FLAGS}")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${XRAY_FLAGS}")

    message (STATUS "Using XRay")
endif()
