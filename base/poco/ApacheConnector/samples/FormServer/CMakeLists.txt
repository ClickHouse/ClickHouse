set(SAMPLE_NAME "FormServer")

set(LOCAL_SRCS "")
aux_source_directory(src LOCAL_SRCS)

add_library( ${SAMPLE_NAME} ${LOCAL_SRCS} )
target_link_libraries( ${SAMPLE_NAME} PocoNet PocoFoundation )
