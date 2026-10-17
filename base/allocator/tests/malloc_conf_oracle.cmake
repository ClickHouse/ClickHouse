# `malloc_conf_oracle` calls jemalloc's `malloc_conf_init` directly, in a fresh child process per configuration string.
# It links a patched copy of the reference library:
# - without `.init_array`, so that jemalloc's constructors do not initialize it (and parse the configuration) at
#   program start;
# - with the `static` arrays of `tcache_ncached_max` made global, so that the parsed values can be read.

find_program (ALLOCATOR_OBJCOPY NAMES llvm-objcopy objcopy REQUIRED)

set (malloc_conf_oracle_reference "${CMAKE_CURRENT_BINARY_DIR}/malloc_conf_oracle_jemalloc.a")
add_custom_command (
    OUTPUT "${malloc_conf_oracle_reference}"
    COMMAND "${ALLOCATOR_OBJCOPY}"
        --remove-section=.init_array
        --globalize-symbol=opt_tcache_ncached_max
        --globalize-symbol=opt_tcache_ncached_max_set
        "${ALLOCATOR_REFERENCE_JEMALLOC}" "${malloc_conf_oracle_reference}"
    DEPENDS "${ALLOCATOR_REFERENCE_JEMALLOC}"
    VERBATIM)
add_custom_target (malloc_conf_oracle_reference DEPENDS "${malloc_conf_oracle_reference}")
add_dependencies (malloc_conf_oracle malloc_conf_oracle_reference)
target_link_libraries (malloc_conf_oracle PRIVATE "${malloc_conf_oracle_reference}")
