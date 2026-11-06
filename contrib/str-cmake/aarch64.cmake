set (str_arm_dir "${CMAKE_CURRENT_SOURCE_DIR}/aarch64")
add_library (str_neon OBJECT
    "${str_arm_dir}/aarch64_neon_bulk.c"
    "${str_arm_dir}/aarch64_neon_extra.c"
    "${str_arm_dir}/aarch64_neon_length.c"
    "${str_arm_dir}/aarch64_neon_membership.c"
    "${str_arm_dir}/aarch64_neon_strings.c"
    "${str_arm_dir}/dispatch.c")
add_library (str_sve OBJECT
    "${str_arm_dir}/aarch64_sve_bulk.c"
    "${str_arm_dir}/aarch64_sve_extra.c"
    "${str_arm_dir}/sve_dispatch.c")

foreach (target str_neon str_sve)
    set_target_properties (${target} PROPERTIES
        INTERPROCEDURAL_OPTIMIZATION FALSE POSITION_INDEPENDENT_CODE TRUE)
    # Preserve compiler unwind data and frame chains. No x86 rewriting pass
    # applies to these objects. Disable loop idioms that call libc recursively.
    target_compile_options (${target} PRIVATE
        -O3 -ffreestanding -fno-builtin -fno-stack-protector -fno-lto
        -ffunction-sections -fdata-sections
        -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer)
endforeach ()
target_compile_options (str_neon PRIVATE -march=armv8-a)
target_compile_options (str_sve PRIVATE -march=armv8-a+sve -msve-vector-bits=scalable)

add_library (str_prefixed STATIC $<TARGET_OBJECTS:str_neon> $<TARGET_OBJECTS:str_sve>)
set_target_properties (str_prefixed PROPERTIES OUTPUT_NAME str-prefixed)
set (str_rename_options)
foreach (primitive memcpy memmove memset memcmp strlen strcpy strcat strcmp strspn strcspn memchr)
    list (APPEND str_rename_options "--redefine-sym=str_${primitive}=${primitive}")
endforeach ()

set (str_archive "${CMAKE_CURRENT_BINARY_DIR}/libstr.a")
add_custom_command (
    OUTPUT "${str_archive}"
    COMMAND "${OBJCOPY_PATH}" ${str_rename_options} $<TARGET_FILE:str_prefixed> "${str_archive}"
    DEPENDS str_prefixed
    COMMAND_EXPAND_LISTS
    VERBATIM)
add_custom_target (str_archive DEPENDS "${str_archive}")
add_library (ch_contrib::str STATIC IMPORTED GLOBAL)
set_target_properties (ch_contrib::str PROPERTIES IMPORTED_LOCATION "${str_archive}")
add_dependencies (ch_contrib::str str_archive)
