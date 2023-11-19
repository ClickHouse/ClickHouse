# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

# Check how the platform supports setting thread name
function(aws_set_thread_name_method target)
    if (APPLE)
        # All Apple platforms we support have the same function, so no need for compile-time check.
        return()
    endif()

    # pthread_setname_np() usually takes 2 args
    target_compile_definitions(${target} PRIVATE -DAWS_PTHREAD_SETNAME_TAKES_2ARGS)
endfunction()
