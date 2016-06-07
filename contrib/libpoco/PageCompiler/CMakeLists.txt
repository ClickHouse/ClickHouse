set(POCO_EXENAME "PageCompiler")

# Sources
file(GLOB SRCS_G "src/*.cpp")
POCO_SOURCES_AUTO( SRCS ${SRCS_G})

add_executable( "${POCO_EXENAME}" ${SRCS} )
set_target_properties( "${POCO_EXENAME}"
    PROPERTIES
    OUTPUT_NAME cpspc
    )

target_link_libraries( "${POCO_EXENAME}" PocoNet PocoUtil PocoXML PocoJSON PocoFoundation)

