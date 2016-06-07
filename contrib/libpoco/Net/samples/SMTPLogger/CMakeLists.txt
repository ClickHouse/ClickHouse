set(SAMPLE_NAME "SMTPLogger")

set(LOCAL_SRCS "")
aux_source_directory(src LOCAL_SRCS)

add_executable( ${SAMPLE_NAME} ${LOCAL_SRCS} )
target_link_libraries( ${SAMPLE_NAME} PocoNet PocoUtil PocoJSON PocoXML PocoFoundation )
