# https://llvm.org/docs/XRay.html

if (TARGET ch_contrib::llvm)
    # The target clang must support XRay, otherwise it should error on invalid option
    # Explicitly disable all XRay modes to avoid linking against xray-basic and xray-fdr.
    # Emit instrumentation only for function-entry and function-exit.
    set (XRAY_FLAGS "-fxray-instrument -fxray-modes=none -fxray-instrumentation-bundle=function")
    set (USE_XRAY 1)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${XRAY_FLAGS}")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${XRAY_FLAGS}")

    # Add the XRay headers globally so that all targets (not just those linking ch_contrib::llvm)
    # can include <xray/xray_interface.h> when USE_XRAY is enabled.
    include_directories (SYSTEM "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/compiler-rt/include")

    message (STATUS "Using LLVM XRay")
else()
    message (STATUS "Not using LLVM XRay because LLVM is not built along")
endif()
