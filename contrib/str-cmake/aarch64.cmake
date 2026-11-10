set (str_arm_dir "${STR_SOURCE_DIR}/aarch64/selected")
add_library (str_neon OBJECT
    "${str_arm_dir}/aarch64_neon_bulk.c"
    "${str_arm_dir}/aarch64_neon_extra.c"
    "${str_arm_dir}/aarch64_neon_length.c"
    "${str_arm_dir}/aarch64_neon_membership.c"
    "${str_arm_dir}/aarch64_neon_strings.c"
    "${str_arm_dir}/dispatch.c"
    "${str_arm_dir}/memory_copy.c"
    "${str_arm_dir}/memory_fill.c"
    "${str_arm_dir}/memory_search.c")
add_library (str_sve OBJECT
    "${str_arm_dir}/aarch64_sve_bulk.c"
    "${str_arm_dir}/aarch64_sve_extra.c"
    "${str_arm_dir}/sve_dispatch.c"
    "${str_arm_dir}/tiny_copy_sve.c"
    "${str_arm_dir}/memory_search_sve.c")

foreach (target str_neon str_sve)
    set_target_properties (${target} PROPERTIES
        INTERPROCEDURAL_OPTIMIZATION FALSE POSITION_INDEPENDENT_CODE TRUE
        LINK_LIBRARIES "")
    # Preserve compiler unwind data and nonleaf frame chains. No x86 rewriting pass
    # applies to these objects. Disable loop idioms that call libc recursively.
    # Whole-program vtables require LTO; override the inherited release flag too.
    target_compile_options (${target} PRIVATE
        -O3 -ffreestanding -fno-builtin -fno-stack-protector -fno-lto -fno-whole-program-vtables
        -ffunction-sections -fdata-sections
        -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer)
endforeach ()
# Public memory entries and small SVE helpers avoid leaf frame bookkeeping
# while retaining compiler-generated CFI. Bulk entries keep their frames.
# This provider is Linux-only without sanitizers.
set_source_files_properties ("${str_arm_dir}/memory_copy.c" "${str_arm_dir}/tiny_copy_sve.c" "${str_arm_dir}/memory_fill.c"
    "${str_arm_dir}/memory_search.c" "${str_arm_dir}/memory_search_sve.c" PROPERTIES
    COMPILE_OPTIONS "-momit-leaf-frame-pointer;-fasynchronous-unwind-tables")
target_compile_options (str_neon PRIVATE -march=armv8-a)
target_compile_options (str_sve PRIVATE -march=armv8-a+sve -msve-vector-bits=scalable)

add_library (str_prefixed STATIC $<TARGET_OBJECTS:str_neon> $<TARGET_OBJECTS:str_sve>)
# The provider itself must not inherit `global-group`, which consumes the
# completed archive. Otherwise its archive-generation dependency is cyclic.
set_target_properties (str_prefixed PROPERTIES
    OUTPUT_NAME str-prefixed LINK_LIBRARIES "" INTERFACE_LINK_LIBRARIES "")
set (str_rename_options)
foreach (primitive memcpy memmove memset memcmp strlen strcpy strcat strcmp strspn strcspn memchr)
    list (APPEND str_rename_options "--redefine-sym=str_${primitive}=${primitive}")
endforeach ()

set (str_archive "${CMAKE_CURRENT_BINARY_DIR}/libstr.a")
add_custom_command (
    OUTPUT "${str_archive}"
    COMMAND ${str_contract_command}
    COMMAND "${OBJCOPY_PATH}" ${str_rename_options} $<TARGET_FILE:str_prefixed> "${str_archive}"
    DEPENDS str_prefixed ${str_contract_sources}
    COMMAND_EXPAND_LISTS
    VERBATIM)
add_custom_target (str_archive DEPENDS "${str_archive}")
add_library (ch_contrib::str STATIC IMPORTED GLOBAL)
set_target_properties (ch_contrib::str PROPERTIES IMPORTED_LOCATION "${str_archive}")
add_dependencies (ch_contrib::str str_archive)
