# https://llvm.org/docs/XRay.html

if (TARGET ch_contrib::llvm)
    # The target clang must support xray, otherwise it should error on invalid option
    set (XRAY_FLAGS "-fxray-instrument")
    set (USE_XRAY 1)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${XRAY_FLAGS}")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${XRAY_FLAGS}")

    message (STATUS "Using LLVM XRay")
else()
    message (STATUS "Not using LLVM XRay because LLVM is not built along")
endif()
