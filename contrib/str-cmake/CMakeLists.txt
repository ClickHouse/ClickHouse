set (STR_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/str")

if (NOT OS_LINUX OR NOT (ARCH_AMD64 OR ARCH_AARCH64) OR SANITIZE OR SANITIZE_COVERAGE)
    message (FATAL_ERROR "pg83/str requires an x86-64 or AArch64 Linux build without sanitizers")
endif ()

if (NOT OBJCOPY_PATH)
    message (FATAL_ERROR "pg83/str requires objcopy to export the libc symbols")
endif ()

if (ARCH_AARCH64)
    include (aarch64.cmake)
    return ()
endif ()

find_package (Python3 REQUIRED COMPONENTS Interpreter)

set (str_objects)
foreach (unit sse2 sse42 avx2 evex evex512 dispatch)
    add_library (str_${unit} OBJECT "${STR_SOURCE_DIR}/${unit}.c")
    # `global-group` links the completed archive into consumers; these objects
    # must not depend on that same archive while producing it.
    set_target_properties (str_${unit} PROPERTIES
        INTERPROCEDURAL_OPTIMIZATION FALSE LINK_LIBRARIES "")
    # The post-pass consumes machine code, so disable LTO and stale source-level debug data.
    # Retain every frame pointer: the post-pass removes unwind information.
    target_compile_options (str_${unit} PRIVATE
        -O3 -fno-builtin -fno-stack-protector -fno-lto -g0 -fPIC
        -ffunction-sections -fdata-sections
        -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer)
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
    COMMAND "${Python3_EXECUTABLE}" "${STR_SOURCE_DIR}/dev/post.py"
        --check --out "${CMAKE_CURRENT_BINARY_DIR}/post"
        --archive "${CMAKE_CURRENT_BINARY_DIR}/libstr-prefixed.a"
        --report "${CMAKE_CURRENT_BINARY_DIR}/post/report.txt"
        --passes fold,rename,tail,hot,thread,ktest,kor,kfirst,lea,copy,tramp,vzu
        ${str_objects}
    COMMAND "${OBJCOPY_PATH}" ${str_rename_options}
        "${CMAKE_CURRENT_BINARY_DIR}/libstr-prefixed.a" "${str_archive}"
    DEPENDS ${str_objects} ${str_post_sources} "${STR_SOURCE_DIR}/dev/phases.json"
    COMMAND_EXPAND_LISTS
    VERBATIM)
add_custom_target (str_archive DEPENDS "${str_archive}")
add_library (ch_contrib::str STATIC IMPORTED GLOBAL)
set_target_properties (ch_contrib::str PROPERTIES IMPORTED_LOCATION "${str_archive}")
add_dependencies (ch_contrib::str str_archive)
