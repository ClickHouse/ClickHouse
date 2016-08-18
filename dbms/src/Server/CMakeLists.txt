add_executable(clickhouse-server
	Server.cpp
	HTTPHandler.cpp
	TCPHandler.cpp
	InterserverIOHTTPHandler.cpp
	MetricsTransmitter.cpp
	UsersConfigReloader.cpp
	StatusFile.cpp
	ReplicasStatusHandler.cpp)

# Adding init.d support
INCLUDE(${PROJECT_SOURCE_DIR}/tools/init.d/CMakeLists.init)

target_link_libraries (clickhouse-server daemon)

INSTALL(TARGETS clickhouse-server RUNTIME DESTINATION bin COMPONENT clickhouse-server)

INSTALL(
    FILES config.xml users.xml
    DESTINATION /etc/clickhouse-server
    COMPONENT clickhouse-server)

INSTALL(
    FILES ${CLICKHOUSE_PRIVATE_DIR}/Server/metrika/config.xml ${CLICKHOUSE_PRIVATE_DIR}/Server/metrika/users.xml
    DESTINATION /etc/clickhouse-server/metrika
    COMPONENT clickhouse-server
    OPTIONAL)

INSTALL(
    FILES metrika.conf
    DESTINATION /etc/security/limits.d
    COMPONENT clickhouse-server)

CREATE_INIT_SCRIPT(clickhouse-server clickhouse-server)

