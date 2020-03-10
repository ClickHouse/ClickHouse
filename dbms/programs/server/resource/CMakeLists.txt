file(GLOB CLICKHOUSE_SERVER_RESOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*")

foreach(resource ${CLICKHOUSE_SERVER_RESOURCES})
    if(NOT ${resource} MATCHES "CMakeLists.txt")
        set(CLICKHOUSE_DATA_BINARYS ${CLICKHOUSE_DATA_BINARYS} "${resource}.o")
        add_custom_command(OUTPUT ${resource}.o
            COMMAND ld -r -b binary -o '${CMAKE_CURRENT_BINARY_DIR}/${resource}.o' ${resource}
            DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${resource}
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    endif()
endforeach()

add_library(server_resource STATIC ${CLICKHOUSE_DATA_BINARYS})
SET_TARGET_PROPERTIES(server_resource PROPERTIES LINKER_LANGUAGE C)
SET_SOURCE_FILES_PROPERTIES(${CLICKHOUSE_DATA_BINARYS} PROPERTIES EXTERNAL_OBJECT true GENERATED true)
