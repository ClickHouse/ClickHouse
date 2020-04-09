configure_file(config_formats.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/config_formats.h)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
