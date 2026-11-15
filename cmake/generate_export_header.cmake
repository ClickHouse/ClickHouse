# Probe-free replacement for CMake's `generate_export_header`.
#
# CMake's stock `generate_export_header` runs `try_compile` checks for hidden visibility
# and the `__attribute__((__deprecated__))` attribute. Build-time CMake checks are
# forbidden in ClickHouse (see `cmake/block_build_time_checks.cmake`); the compiler is
# fixed (Clang, see `cmake/tools.cmake`), and both features are statically known to be
# supported, so we emit the export header directly.
#
# Supported keyword arguments mirror the subset used by the contrib code:
#   BASE_NAME                    base name for derived macro names
#   EXPORT_MACRO_NAME            override the export macro name
#   NO_EXPORT_MACRO_NAME         override the no-export macro name
#   DEPRECATED_MACRO_NAME        override the deprecated macro name
#   STATIC_DEFINE                macro that, when defined, makes the export macros expand to nothing
#   EXPORT_FILE_NAME             output path (relative paths resolve against the current binary dir)
#   CUSTOM_CONTENT_FROM_VARIABLE name of a variable whose contents are appended to the file
#   INCLUDE_GUARD_NAME           override the include-guard macro name

function (clickhouse_generate_export_header TARGET_LIBRARY)
    set (oneValueArgs
        BASE_NAME
        EXPORT_MACRO_NAME
        NO_EXPORT_MACRO_NAME
        DEPRECATED_MACRO_NAME
        STATIC_DEFINE
        EXPORT_FILE_NAME
        CUSTOM_CONTENT_FROM_VARIABLE
        INCLUDE_GUARD_NAME)
    cmake_parse_arguments (ARG "" "${oneValueArgs}" "" ${ARGN})

    set (BASE_NAME "${TARGET_LIBRARY}")
    if (ARG_BASE_NAME)
        set (BASE_NAME "${ARG_BASE_NAME}")
    endif ()
    string (TOUPPER "${BASE_NAME}" BASE_NAME_UPPER)

    set (EXPORT_MACRO_NAME "${BASE_NAME_UPPER}_EXPORT")
    if (ARG_EXPORT_MACRO_NAME)
        set (EXPORT_MACRO_NAME "${ARG_EXPORT_MACRO_NAME}")
    endif ()

    set (NO_EXPORT_MACRO_NAME "${BASE_NAME_UPPER}_NO_EXPORT")
    if (ARG_NO_EXPORT_MACRO_NAME)
        set (NO_EXPORT_MACRO_NAME "${ARG_NO_EXPORT_MACRO_NAME}")
    endif ()

    set (DEPRECATED_MACRO_NAME "${BASE_NAME_UPPER}_DEPRECATED")
    if (ARG_DEPRECATED_MACRO_NAME)
        set (DEPRECATED_MACRO_NAME "${ARG_DEPRECATED_MACRO_NAME}")
    endif ()

    set (STATIC_DEFINE "${BASE_NAME_UPPER}_STATIC_DEFINE")
    if (ARG_STATIC_DEFINE)
        set (STATIC_DEFINE "${ARG_STATIC_DEFINE}")
    endif ()

    if (ARG_INCLUDE_GUARD_NAME)
        set (INCLUDE_GUARD_NAME "${ARG_INCLUDE_GUARD_NAME}")
    else ()
        set (INCLUDE_GUARD_NAME "${EXPORT_MACRO_NAME}_H")
    endif ()

    if (ARG_EXPORT_FILE_NAME)
        if (IS_ABSOLUTE "${ARG_EXPORT_FILE_NAME}")
            set (EXPORT_FILE_NAME "${ARG_EXPORT_FILE_NAME}")
        else ()
            set (EXPORT_FILE_NAME "${CMAKE_CURRENT_BINARY_DIR}/${ARG_EXPORT_FILE_NAME}")
        endif ()
    else ()
        string (TOLOWER "${BASE_NAME}" BASE_NAME_LOWER)
        set (EXPORT_FILE_NAME "${CMAKE_CURRENT_BINARY_DIR}/${BASE_NAME_LOWER}_export.h")
    endif ()

    get_target_property (EXPORT_IMPORT_CONDITION "${TARGET_LIBRARY}" DEFINE_SYMBOL)
    if (NOT EXPORT_IMPORT_CONDITION)
        set (EXPORT_IMPORT_CONDITION "${TARGET_LIBRARY}_EXPORTS")
    endif ()

    # Mirror CMake's `_do_set_macro_values`: for static libraries the visibility
    # attributes are left empty - the macros never expand to anything visible. For
    # shared / module / object libraries, set them based on Clang's hidden-visibility
    # support (which is statically known to be available - see `cmake/tools.cmake`).
    set (DEFINE_EXPORT "")
    set (DEFINE_IMPORT "")
    set (DEFINE_NO_EXPORT "")
    get_target_property (_target_type "${TARGET_LIBRARY}" TYPE)
    if (NOT _target_type STREQUAL "STATIC_LIBRARY")
        set (DEFINE_EXPORT "__attribute__((visibility(\"default\")))")
        set (DEFINE_IMPORT "__attribute__((visibility(\"default\")))")
        set (DEFINE_NO_EXPORT "__attribute__((visibility(\"hidden\")))")
    endif ()

    set (CUSTOM_CONTENT "")
    if (ARG_CUSTOM_CONTENT_FROM_VARIABLE AND DEFINED "${ARG_CUSTOM_CONTENT_FROM_VARIABLE}")
        set (CUSTOM_CONTENT "${${ARG_CUSTOM_CONTENT_FROM_VARIABLE}}")
    endif ()

    configure_file (
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/generate_export_header.h.in"
        "${EXPORT_FILE_NAME}"
        @ONLY)
endfunction ()
