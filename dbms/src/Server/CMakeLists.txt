add_library(clickhouse-server
	Server.cpp
	HTTPHandler.cpp
	TCPHandler.cpp
	InterserverIOHTTPHandler.cpp
	MetricsTransmitter.cpp
	ConfigReloader.cpp
	StatusFile.cpp
	ReplicasStatusHandler.cpp
)
target_link_libraries(clickhouse-server daemon)

add_library(clickhouse-local LocalServer.cpp)
target_link_libraries(clickhouse-local dbms)

add_executable(clickhouse main.cpp)
target_link_libraries(clickhouse clickhouse-server clickhouse-client clickhouse-local clickhouse-benchmark)
INSTALL(TARGETS clickhouse RUNTIME DESTINATION bin COMPONENT clickhouse)

# make symbolic links to concrete clickhouse applications
macro(install_symlink_to_clickhouse app)
	INSTALL(CODE "execute_process(COMMAND ln -sf clickhouse ${app} WORKING_DIRECTORY \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/bin\" )" COMPONENT ${app})
endmacro(install_symlink_to_clickhouse)

install_symlink_to_clickhouse(clickhouse-server)
install_symlink_to_clickhouse(clickhouse-client)
install_symlink_to_clickhouse(clickhouse-local)
install_symlink_to_clickhouse(clickhouse-benchmark)

INSTALL(
    FILES config.xml users.xml
    DESTINATION  ${CLICKHOUSE_ETC_DIR}/clickhouse-server
    COMPONENT clickhouse)

INSTALL(
    FILES ${CLICKHOUSE_PRIVATE_DIR}/Server/metrika/config.xml ${CLICKHOUSE_PRIVATE_DIR}/Server/metrika/users.xml
    DESTINATION ${CLICKHOUSE_ETC_DIR}/clickhouse-server/metrika
    COMPONENT clickhouse
    OPTIONAL)

if (NOT CMAKE_SYSTEM MATCHES "FreeBSD")
	INSTALL(
		FILES metrika.conf
		DESTINATION /etc/security/limits.d
		COMPONENT clickhouse)

	# Adding init.d support
	include (${ClickHouse_SOURCE_DIR}/cmake/create_init_script.cmake)
	create_init_script (clickhouse-server clickhouse-server)
endif ()
