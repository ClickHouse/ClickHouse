set(POCO_EXENAME "PocoDoc")

# Sources
file(GLOB SRCS_G "src/*.cpp")
POCO_SOURCES_AUTO( SRCS ${SRCS_G})

add_executable( "${POCO_EXENAME}" ${SRCS} )

target_link_libraries( "${POCO_EXENAME}" Util XML Foundation CppParser)

install(
    TARGETS "${POCO_EXENAME}" EXPORT "${POCO_EXENAME}Targets"
    LIBRARY DESTINATION lib${LIB_SUFFIX}
    ARCHIVE DESTINATION lib${LIB_SUFFIX}
    RUNTIME DESTINATION bin
    INCLUDES DESTINATION include
    )
