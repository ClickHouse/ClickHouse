include_directories(
	.
	../include
	../include/compat
)

set(
	TLS_SRC
	tls.c
	tls_bio_cb.c
	tls_client.c
	tls_config.c
	tls_conninfo.c
	tls_server.c
	tls_ocsp.c
	tls_peer.c
	tls_util.c
	tls_verify.c
)


if(NOT "${OPENSSLDIR}" STREQUAL "")
	add_definitions(-D_PATH_SSL_CA_FILE=\"${OPENSSLDIR}/cert.pem\")
else()
	add_definitions(-D_PATH_SSL_CA_FILE=\"${CMAKE_INSTALL_PREFIX}/etc/ssl/cert.pem\")
endif()

add_library(tls-objects OBJECT ${TLS_SRC})
if (BUILD_SHARED)
	add_library(tls STATIC $<TARGET_OBJECTS:tls-objects>)
	add_library(tls-shared SHARED $<TARGET_OBJECTS:tls-objects>)
	export_symbol(tls-shared ${CMAKE_CURRENT_SOURCE_DIR}/tls.sym)
	target_link_libraries(tls-shared ssl-shared crypto-shared)
	if (WIN32)
		target_link_libraries(tls-shared Ws2_32.lib)
		set(TLS_POSTFIX -${TLS_MAJOR_VERSION})
	endif()
	set_target_properties(tls-shared PROPERTIES
		OUTPUT_NAME tls${TLS_POSTFIX}
		ARCHIVE_OUTPUT_NAME tls${TLS_POSTFIX})
	set_target_properties(tls-shared PROPERTIES VERSION ${TLS_VERSION}
		SOVERSION ${TLS_MAJOR_VERSION})
	if(ENABLE_LIBRESSL_INSTALL)
		install(TARGETS tls tls-shared DESTINATION ${CMAKE_INSTALL_LIBDIR})
	endif(ENABLE_LIBRESSL_INSTALL)
else()
	add_library(tls STATIC ${TLS_SRC})
	if(ENABLE_LIBRESSL_INSTALL)
		install(TARGETS tls DESTINATION ${CMAKE_INSTALL_LIBDIR})
	endif(ENABLE_LIBRESSL_INSTALL)
endif()

