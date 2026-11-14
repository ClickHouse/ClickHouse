# These executables link the prefixed production archive with the host libc.
# Keep them outside `global-group`, which consumes the public archive itself.
foreach (required STR_CONTRACT_NATIVE STR_CONTRACT_ARCH STR_CONTRACT_CC
        STR_CONTRACT_SOURCE STR_CONTRACT_ARCHIVE STR_CONTRACT_OUT)
    if (NOT DEFINED ${required})
        message (FATAL_ERROR "Missing archive-contract argument: ${required}")
    endif ()
endforeach ()

if (NOT STR_CONTRACT_NATIVE)
    message (STATUS "pg83str native archive contracts skipped: target-native Linux execution is unavailable")
    return ()
endif ()

string (TIMESTAMP contract_time "%Y-%m-%dT%H-%M-%SZ" UTC)
string (RANDOM LENGTH 8 ALPHABET 0123456789abcdef contract_nonce)
set (contract_dir "${STR_CONTRACT_OUT}/${contract_time}-${contract_nonce}")
file (MAKE_DIRECTORY "${contract_dir}")
file (SHA256 "${STR_CONTRACT_ARCHIVE}" archive_before)

function (run_contract step budget)
    set (log "${contract_dir}/test_${step}.log")
    execute_process (
        COMMAND ${ARGN}
        RESULT_VARIABLE result
        OUTPUT_FILE "${log}"
        ERROR_FILE "${log}"
        TIMEOUT ${budget})
    if (NOT "${result}" STREQUAL "0")
        file (SIZE "${log}" log_size)
        math (EXPR log_offset "${log_size} - 8192")
        if (log_offset LESS 0)
            set (log_offset 0)
        endif ()
        file (READ "${log}" log_tail OFFSET ${log_offset} LIMIT 8192)
        message (FATAL_ERROR "pg83str ${step} failed (${result}); see ${log}\n${log_tail}")
    endif ()
endfunction ()

set (test_flags -std=gnu11 -O2 -fno-builtin -UNDEBUG "-I${STR_CONTRACT_SOURCE}")
if (STR_CONTRACT_ARCH STREQUAL "amd64")
    run_contract (compile_api 120 "${STR_CONTRACT_CC}" ${test_flags}
        "${STR_CONTRACT_SOURCE}/tst/test.c" "${STR_CONTRACT_ARCHIVE}"
        -o "${contract_dir}/api")
    run_contract (compile_cached_bulk 120 "${STR_CONTRACT_CC}" ${test_flags}
        "${STR_CONTRACT_SOURCE}/tst/cached_bulk.c" "${STR_CONTRACT_ARCHIVE}"
        -ldl -o "${contract_dir}/cached-bulk")
    run_contract (cached_bulk 300 "${contract_dir}/cached-bulk")
    foreach (profile IN ITEMS evex512i evex512ri)
        run_contract (cached_bulk_${profile} 300 "${contract_dir}/cached-bulk" "${profile}")
    endforeach ()
    # Include large sizes, overlaps and random inputs for every executable
    # configuration, followed by all one-slot mixes.
    run_contract (api 1800 "${contract_dir}/api")
    file (STRINGS "${contract_dir}/test_api.log" dispatched_profile REGEX "^dispatched [(]")
    list (LENGTH dispatched_profile dispatched_profile_count)
    if (NOT dispatched_profile_count EQUAL 1)
        message (FATAL_ERROR "Missing or ambiguous native dispatch result in ${contract_dir}/test_api.log")
    endif ()
    message (STATUS "pg83str ${dispatched_profile}")
    run_contract (public_api 300 "${contract_dir}/api" --public-quick)
    run_contract (compile_dispatch 120 "${STR_CONTRACT_CC}" ${test_flags} -pthread
        "${STR_CONTRACT_SOURCE}/tst/dispatch_threads.c" "${STR_CONTRACT_ARCHIVE}"
        -o "${contract_dir}/dispatch")
    run_contract (dispatch 60 "${contract_dir}/dispatch")
elseif (STR_CONTRACT_ARCH STREQUAL "arm64")
    run_contract (compile_api 120 "${STR_CONTRACT_CC}" ${test_flags} -march=armv8-a
        "${STR_CONTRACT_SOURCE}/tst/aarch64_selected.c" "${STR_CONTRACT_ARCHIVE}"
        -o "${contract_dir}/api")
    run_contract (api 300 "${contract_dir}/api")
    run_contract (compile_neon 120 "${STR_CONTRACT_CC}" ${test_flags} -march=armv8-a
        -DSTR_TEST_NO_SVE -Wl,--wrap=getauxval
        "${STR_CONTRACT_SOURCE}/tst/aarch64_selected.c" "${STR_CONTRACT_ARCHIVE}"
        -o "${contract_dir}/neon")
    run_contract (neon 300 "${contract_dir}/neon")
else ()
    message (FATAL_ERROR "Unknown archive-contract architecture: ${STR_CONTRACT_ARCH}")
endif ()

file (SHA256 "${STR_CONTRACT_ARCHIVE}" archive_after)
if (NOT archive_before STREQUAL archive_after)
    message (FATAL_ERROR "pg83str archive changed during contract validation")
endif ()
file (WRITE "${contract_dir}/receipt.txt"
    "status=PASS\narchitecture=${STR_CONTRACT_ARCH}\nprefixed_archive_sha256=${archive_after}\ncompiler=${STR_CONTRACT_CC}\n")
message (STATUS "pg83str native archive contracts passed: ${contract_dir}")
