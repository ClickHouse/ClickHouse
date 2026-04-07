# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

# Check if the platform supports setting thread affinity
# (important for hitting full NIC entitlement on NUMA architectures)
function(aws_set_thread_affinity_method target)
    # This code has been cut, because I don't care about it.
    target_compile_definitions(${target} PRIVATE -DAWS_AFFINITY_METHOD=AWS_AFFINITY_METHOD_NONE)
endfunction()
