# Embed a set of resource files into a resulting object file.
#
# Signature: `clickhouse_embed_binaries(TARGET <target> RESOURCE_DIR <dir> RESOURCES <resource> ...)
#
# This will generate a static library target named `<target>`, which contains the contents of
# each `<resource>` file. The files should be located in `<dir>`. <dir> defaults to
# ${CMAKE_CURRENT_SOURCE_DIR}, and the resources may not be empty.
#
# Each resource will result in three symbols in the final archive, based on the name `<resource>`.
# These are:
#   1. `_binary_<name>_start`: Points to the start of the binary data from `<resource>`.
#   2. `_binary_<name>_end`: Points to the end of the binary data from `<resource>`.
#   2. `_binary_<name>_size`: Points to the size of the binary data from `<resource>`.
#
# `<name>` is a normalized name derived from `<resource>`, by replacing the characters "./-" with
# the character "_", and the character "+" with "_PLUS_". This scheme is similar to those generated
# by `ld -r -b binary`, and matches the expectations in `./base/common/getResource.cpp`.
macro(clickhouse_embed_binaries)
    set(one_value_args TARGET RESOURCE_DIR)
    set(resources RESOURCES)
    cmake_parse_arguments(EMBED "" "${one_value_args}" ${resources} ${ARGN})

    if (NOT DEFINED EMBED_TARGET)
        message(FATAL_ERROR "A target name must be provided for embedding binary resources into")
    endif()

    if (NOT DEFINED EMBED_RESOURCE_DIR)
        set(EMBED_RESOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
    endif()

    list(LENGTH EMBED_RESOURCES N_RESOURCES)
    if (N_RESOURCES LESS 1)
        message(FATAL_ERROR "The list of binary resources to embed may not be empty")
    endif()

    add_library("${EMBED_TARGET}" STATIC)
    set_target_properties("${EMBED_TARGET}" PROPERTIES LINKER_LANGUAGE C)

    set(EMBED_TEMPLATE_FILE "${PROJECT_SOURCE_DIR}/programs/embed_binary.S.in")

    foreach(RESOURCE_FILE ${EMBED_RESOURCES})
        set(ASSEMBLY_FILE_NAME "${RESOURCE_FILE}.S")
        set(BINARY_FILE_NAME "${RESOURCE_FILE}")

        # Normalize the name of the resource.
        string(REGEX REPLACE "[\./-]" "_" SYMBOL_NAME "${RESOURCE_FILE}") # - must be last in regex
        string(REPLACE "+" "_PLUS_" SYMBOL_NAME "${SYMBOL_NAME}")

        # Generate the configured assembly file in the output directory.
        configure_file("${EMBED_TEMPLATE_FILE}" "${CMAKE_CURRENT_BINARY_DIR}/${ASSEMBLY_FILE_NAME}" @ONLY)

        # Set the include directory for relative paths specified for `.incbin` directive.
        set_property(SOURCE "${CMAKE_CURRENT_BINARY_DIR}/${ASSEMBLY_FILE_NAME}" APPEND PROPERTY INCLUDE_DIRECTORIES "${EMBED_RESOURCE_DIR}")

        target_sources("${EMBED_TARGET}" PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/${ASSEMBLY_FILE_NAME}")
        set_target_properties("${EMBED_TARGET}" PROPERTIES OBJECT_DEPENDS "${RESOURCE_FILE}")
    endforeach()
endmacro()
