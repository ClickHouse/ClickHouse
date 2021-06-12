configure_file(config_formats.h.in ${ConfigIncludePath}/config_formats.h)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
