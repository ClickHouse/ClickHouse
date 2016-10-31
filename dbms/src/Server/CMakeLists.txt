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
target_link_libraries(clickhouse clickhouse-server clickhouse-client clickhouse-local)
INSTALL(TARGETS clickhouse RUNTIME DESTINATION bin COMPONENT clickhouse)

#make symbolic links to concrete clickhouse applications
INSTALL(CODE "execute_process(COMMAND ln -s -f clickhouse clickhouse-server WORKING_DIRECTORY \"${CMAKE_INSTALL_PREFIX}/bin\")")
INSTALL(CODE "execute_process(COMMAND ln -s -f clickhouse clickhouse-client WORKING_DIRECTORY \"${CMAKE_INSTALL_PREFIX}/bin\")")
INSTALL(CODE "execute_process(COMMAND ln -s -f clickhouse clickhouse-local WORKING_DIRECTORY \"${CMAKE_INSTALL_PREFIX}/bin\")")

INSTALL(
    FILES config.xml users.xml
    DESTINATION /etc/clickhouse-server
    COMPONENT clickhouse)

INSTALL(
    FILES ${CLICKHOUSE_PRIVATE_DIR}/Server/metrika/config.xml ${CLICKHOUSE_PRIVATE_DIR}/Server/metrika/users.xml
    DESTINATION /etc/clickhouse-server/metrika
    COMPONENT clickhouse
    OPTIONAL)

INSTALL(
    FILES metrika.conf
    DESTINATION /etc/security/limits.d
    COMPONENT clickhouse)

# Adding init.d support
INCLUDE(${PROJECT_SOURCE_DIR}/tools/init.d/CMakeLists.init)
CREATE_INIT_SCRIPT(clickhouse-server clickhouse-server)
