set(SAMPLE_NAME "Mail-ssl")

set(LOCAL_SRCS "")
aux_source_directory(src LOCAL_SRCS)

add_executable( ${SAMPLE_NAME} ${LOCAL_SRCS} )
target_link_libraries( ${SAMPLE_NAME} PocoNetSSL PocoCrypto PocoUtil PocoNet PocoXML PocoFoundation )
