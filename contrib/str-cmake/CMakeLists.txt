set (STR_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/str")

if (NOT OS_LINUX OR NOT (ARCH_AMD64 OR ARCH_AARCH64) OR SANITIZE OR SANITIZE_COVERAGE OR WITH_COVERAGE OR ENABLE_FUZZING OR FUZZER OR ENABLE_CLICKHOUSE_PGO_GENERATE)
    message (FATAL_ERROR "pg83/str requires an x86-64 or AArch64 Linux build without sanitizer, coverage, fuzzer or PGO-generation instrumentation")
endif ()

if (NOT OBJCOPY_PATH)
    message (FATAL_ERROR "pg83/str requires objcopy to export the libc symbols")
endif ()

set (str_native_contracts OFF)
if (CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
    string (TOLOWER "${CMAKE_HOST_SYSTEM_PROCESSOR}" str_host_processor)
    if ((ARCH_AMD64 AND str_host_processor MATCHES "^(x86_64|amd64)$") OR
        (ARCH_AARCH64 AND str_host_processor MATCHES "^(aarch64|arm64)$"))
        set (str_native_contracts ON)
    endif ()
endif ()
if (ARCH_AMD64)
    set (str_contract_arch amd64)
else ()
    set (str_contract_arch arm64)
endif ()
set (str_contract_command
    "${CMAKE_COMMAND}"
    "-DSTR_CONTRACT_NATIVE=${str_native_contracts}"
    "-DSTR_CONTRACT_ARCH=${str_contract_arch}"
    "-DSTR_CONTRACT_CC=${CMAKE_C_COMPILER}"
    "-DSTR_CONTRACT_SOURCE=${STR_SOURCE_DIR}"
    "-DSTR_CONTRACT_ARCHIVE=${CMAKE_CURRENT_BINARY_DIR}/libstr-prefixed.a"
    "-DSTR_CONTRACT_OUT=${CMAKE_CURRENT_BINARY_DIR}/contract-tests"
    -P "${CMAKE_CURRENT_SOURCE_DIR}/native_contracts.cmake")
set (str_contract_sources
    "${CMAKE_CURRENT_SOURCE_DIR}/native_contracts.cmake"
    "${STR_SOURCE_DIR}/tst/test.c"
    "${STR_SOURCE_DIR}/tst/cached_bulk.c"
    "${STR_SOURCE_DIR}/tst/dispatch_threads.c"
    "${STR_SOURCE_DIR}/tst/aarch64_selected.c")

if (ARCH_AARCH64)
    include (aarch64.cmake)
    return ()
endif ()

find_program (STR_PYTHON_EXECUTABLE NAMES python3 REQUIRED NO_CMAKE_FIND_ROOT_PATH)

set (str_objects)
foreach (unit sse2 sse42 avx2 evex evex512 dispatch)
    add_library (str_${unit} OBJECT "${STR_SOURCE_DIR}/${unit}.c")
    # `global-group` links the completed archive into consumers; these objects
    # must not depend on that same archive while producing it.
    set_target_properties (str_${unit} PROPERTIES
        INTERPROCEDURAL_OPTIMIZATION FALSE LINK_LIBRARIES "")
    # The post-pass consumes machine code, so disable LTO and stale source-level debug data.
    # Whole-program vtables require LTO; override the inherited release flag too.
    # Regenerate unwind rows after rewriting; retain the compiler's frame effects.
    target_compile_options (str_${unit} PRIVATE
        -O3 -fno-builtin -fno-stack-protector -fno-lto -fno-whole-program-vtables -g0 -fPIC
        -ffunction-sections -fdata-sections
        -fno-omit-frame-pointer -momit-leaf-frame-pointer -fasynchronous-unwind-tables)
    if (unit STREQUAL "sse2" OR unit STREQUAL "dispatch")
        target_compile_options (str_${unit} PRIVATE -march=x86-64)
    elseif (unit STREQUAL "sse42")
        target_compile_options (str_${unit} PRIVATE -march=x86-64-v2)
    elseif (unit STREQUAL "avx2")
        target_compile_options (str_${unit} PRIVATE -march=x86-64-v3)
    else ()
        target_compile_options (str_${unit} PRIVATE -march=x86-64-v4)
        if (unit STREQUAL "evex")
            target_compile_options (str_${unit} PRIVATE -mprefer-vector-width=256)
        else ()
            target_compile_options (str_${unit} PRIVATE -mprefer-vector-width=512)
        endif ()
    endif ()
    list (APPEND str_objects $<TARGET_OBJECTS:str_${unit}>)
endforeach ()

# Rename after specialization: the upstream pass recognizes calls by their str_ names.
set (str_rename_options)
foreach (primitive memcpy memmove memset memcmp strlen strcpy strcat strcmp strspn strcspn memchr)
    list (APPEND str_rename_options "--redefine-sym=str_${primitive}=${primitive}")
endforeach ()

file (GLOB str_post_sources CONFIGURE_DEPENDS "${STR_SOURCE_DIR}/dev/*.py")
set (str_archive "${CMAKE_CURRENT_BINARY_DIR}/libstr.a")
add_custom_command (
    OUTPUT "${str_archive}"
    BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/libstr-prefixed.a" "${CMAKE_CURRENT_BINARY_DIR}/post/report.txt"
    COMMAND "${STR_PYTHON_EXECUTABLE}" "${STR_SOURCE_DIR}/dev/post.py"
        --roundtrip --check --out "${CMAKE_CURRENT_BINARY_DIR}/roundtrip"
        ${str_objects}
    COMMAND "${STR_PYTHON_EXECUTABLE}" "${STR_SOURCE_DIR}/dev/post.py"
        --check --unwind --out "${CMAKE_CURRENT_BINARY_DIR}/post"
        --archive "${CMAKE_CURRENT_BINARY_DIR}/libstr-prefixed.a"
        --report "${CMAKE_CURRENT_BINARY_DIR}/post/report.txt"
        --passes fold,rename,tail,hot,thread,ktest,kor,kfirst,lea,copy,tramp,vzu
        ${str_objects}
    COMMAND ${str_contract_command}
    COMMAND "${OBJCOPY_PATH}" ${str_rename_options}
        "${CMAKE_CURRENT_BINARY_DIR}/libstr-prefixed.a" "${str_archive}"
    DEPENDS ${str_objects} ${str_post_sources} ${str_contract_sources} "${STR_SOURCE_DIR}/dev/phases.json"
    COMMAND_EXPAND_LISTS
    VERBATIM)
add_custom_target (str_archive DEPENDS "${str_archive}")
add_library (ch_contrib::str STATIC IMPORTED GLOBAL)
set_target_properties (ch_contrib::str PROPERTIES IMPORTED_LOCATION "${str_archive}")
add_dependencies (ch_contrib::str str_archive)
