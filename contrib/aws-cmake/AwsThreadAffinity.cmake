# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

include(CheckSymbolExists)

# Check if the platform supports setting thread affinity
# (important for hitting full NIC entitlement on NUMA architectures)
function(aws_set_thread_affinity_method target)

    # Non-POSIX, Android, and Apple platforms do not support thread affinity.
    if (NOT UNIX OR ANDROID OR APPLE)
        target_compile_definitions(${target} PRIVATE
            -DAWS_AFFINITY_METHOD=AWS_AFFINITY_METHOD_NONE)
        return()
    endif()

    cmake_push_check_state()
    list(APPEND CMAKE_REQUIRED_DEFINITIONS -D_GNU_SOURCE)
    list(APPEND CMAKE_REQUIRED_LIBRARIES pthread)

    set(headers "pthread.h")
    # BSDs put nonportable pthread declarations in a separate header.
    if(CMAKE_SYSTEM_NAME MATCHES BSD)
        set(headers "${headers};pthread_np.h")
    endif()

    # Using pthread attrs is the preferred method, but is glibc-specific.
    check_symbol_exists(pthread_attr_setaffinity_np "${headers}" USE_PTHREAD_ATTR_SETAFFINITY)
    if (USE_PTHREAD_ATTR_SETAFFINITY)
        target_compile_definitions(${target} PRIVATE
            -DAWS_AFFINITY_METHOD=AWS_AFFINITY_METHOD_PTHREAD_ATTR)
        return()
    endif()

    # This method is still nonportable, but is supported by musl and BSDs.
    check_symbol_exists(pthread_setaffinity_np "${headers}" USE_PTHREAD_SETAFFINITY)
    if (USE_PTHREAD_SETAFFINITY)
        target_compile_definitions(${target} PRIVATE
            -DAWS_AFFINITY_METHOD=AWS_AFFINITY_METHOD_PTHREAD)
        return()
    endif()

    # If we got here, we expected thread affinity support but didn't find it.
    # We still build with degraded NUMA performance, but show a warning.
    message(WARNING "No supported method for setting thread affinity")
    target_compile_definitions(${target} PRIVATE
        -DAWS_AFFINITY_METHOD=AWS_AFFINITY_METHOD_NONE)

    cmake_pop_check_state()
endfunction()
