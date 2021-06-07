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

    set(EMBED_TEMPLATE_FILE "${PROJECT_SOURCE_DIR}/programs/embed_binary.S.in")
    set(RESOURCE_OBJS)
    foreach(RESOURCE_FILE ${EMBED_RESOURCES})
        set(RESOURCE_OBJ "${RESOURCE_FILE}.o")
        list(APPEND RESOURCE_OBJS "${RESOURCE_OBJ}")

        # Normalize the name of the resource
        set(BINARY_FILE_NAME "${RESOURCE_FILE}")
        string(REGEX REPLACE "[\./-]" "_" SYMBOL_NAME "${RESOURCE_FILE}") # - must be last in regex
        string(REPLACE "+" "_PLUS_" SYMBOL_NAME "${SYMBOL_NAME}")
        set(ASSEMBLY_FILE_NAME "${RESOURCE_FILE}.S")

        # Put the configured assembly file in the output directory.
        # This is so we can clean it up as usual, and we CD to the
        # source directory before compiling, so that the assembly
        # `.incbin` directive can find the file.
        configure_file("${EMBED_TEMPLATE_FILE}" "${CMAKE_CURRENT_BINARY_DIR}/${ASSEMBLY_FILE_NAME}" @ONLY)

        # Generate the output object file by compiling the assembly, in the directory of
        # the sources so that the resource file may also be found
        add_custom_command(
            OUTPUT ${RESOURCE_OBJ}
            COMMAND cd "${EMBED_RESOURCE_DIR}" &&
                ${CMAKE_C_COMPILER} -c -o
                    "${CMAKE_CURRENT_BINARY_DIR}/${RESOURCE_OBJ}"
                    "${CMAKE_CURRENT_BINARY_DIR}/${ASSEMBLY_FILE_NAME}"
        )
        set_source_files_properties("${RESOURCE_OBJ}" PROPERTIES EXTERNAL_OBJECT true GENERATED true)
    endforeach()

    add_library("${EMBED_TARGET}" STATIC ${RESOURCE_OBJS})
    set_target_properties("${EMBED_TARGET}" PROPERTIES LINKER_LANGUAGE C)
endmacro()
