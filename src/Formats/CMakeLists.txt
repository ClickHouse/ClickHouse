configure_file(config_formats.h.in ${ConfigIncludePath}/config_formats.h)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()
