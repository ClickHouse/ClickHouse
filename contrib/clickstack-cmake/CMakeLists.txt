set(CLICKSTACK_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/clickstack/out")

# Set clickstack directory for gzipped files in build directory
set(CLICKSTACK_DIR "${CMAKE_CURRENT_BINARY_DIR}/clickstack_compressed")
file(MAKE_DIRECTORY "${CLICKSTACK_DIR}")

# Generated file location
set(CLICKSTACK_RESOURCES_GENERATED "${CMAKE_CURRENT_BINARY_DIR}/ClickStackResources.generated.h")

# Collect all source files for dependencies
file(GLOB_RECURSE CLICKSTACK_FILES
    CONFIGURE_DEPENDS
    "${CLICKSTACK_SOURCE_DIR}/*"
)

# Custom command to gzip files and generate header
add_custom_command(
    OUTPUT "${CLICKSTACK_RESOURCES_GENERATED}"
    COMMAND ${CMAKE_COMMAND}
        -DCLICKSTACK_SOURCE_DIR="${CLICKSTACK_SOURCE_DIR}"
        -DCLICKSTACK_DIR="${CLICKSTACK_DIR}"
        -DGENERATED_FILE="${CLICKSTACK_RESOURCES_GENERATED}"
        -P "${CMAKE_CURRENT_LIST_DIR}/embed_clickstack_resources.cmake"
    DEPENDS
        "${CMAKE_CURRENT_LIST_DIR}/embed_clickstack_resources.cmake"
        ${CLICKSTACK_FILES}
    WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
    COMMENT "Generating ClickStack embedded resources"
)

# Create a custom target to ensure the file is generated
add_custom_target(clickstack_resources_gen
    DEPENDS "${CLICKSTACK_RESOURCES_GENERATED}"
)

# Create interface library
add_library(_clickstack_resources INTERFACE)
target_include_directories(_clickstack_resources SYSTEM INTERFACE "${CMAKE_CURRENT_BINARY_DIR}")
add_dependencies(_clickstack_resources clickstack_resources_gen)

add_library(ch_contrib::clickstack_resources ALIAS _clickstack_resources)
