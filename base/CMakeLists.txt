if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

if (WITH_COVERAGE)
    # Can't add for whole folder as daemon/ needs instrumentation
    #add_compile_options(-O3 -DNDEBUG -finline-functions -finline-hint-functions ${WITHOUT_COVERAGE})

    set_source_files_properties(
        ext/chrono_io.h
        ext/scope_guard.h
        ext/shared_ptr_helper.h
        PROPERTIES COMPILE_FLAGS "-O3 -finline-functions ${WITHOUT_COVERAGE}")
endif()

add_subdirectory (common)
add_subdirectory (daemon)
add_subdirectory (loggers)
add_subdirectory (pcg-random)
add_subdirectory (widechar_width)
add_subdirectory (readpassphrase)
add_subdirectory (bridge)

if (USE_MYSQL)
    add_subdirectory (mysqlxx)
endif ()
