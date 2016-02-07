add_executable(clickhouse-server
	Server.cpp
	HTTPHandler.cpp
	TCPHandler.cpp
	InterserverIOHTTPHandler.cpp
	OLAPHTTPHandler.cpp
	OLAPQueryParser.cpp
	OLAPQueryConverter.cpp
	MetricsTransmitter.cpp
	UsersConfigReloader.cpp
	StatusFile.cpp
	ReplicasStatusHandler.cpp)

# Adding init.d and logrotate support
INCLUDE(${PROJECT_SOURCE_DIR}/tools/init.d/CMakeLists.init)
INCLUDE(${PROJECT_SOURCE_DIR}/tools/logrotate/CMakeLists.logrotate)

target_link_libraries (clickhouse-server daemon)

INSTALL(TARGETS clickhouse-server RUNTIME DESTINATION bin COMPONENT clickhouse-server)
INSTALL(FILES config.xml users.xml DESTINATION /etc/clickhouse-server COMPONENT clickhouse-server)
INSTALL(FILES metrika/config.xml metrika/users.xml DESTINATION /etc/clickhouse-server/metrika COMPONENT clickhouse-server)
INSTALL(FILES metrika.conf DESTINATION /etc/security/limits.d COMPONENT clickhouse-server)

INSTALL(FILES clickhouse-server.logrt DESTINATION /etc/logrotate.d
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    COMPONENT clickhouse-server)

CREATE_INIT_SCRIPT(clickhouse-server clickhouse-server)

