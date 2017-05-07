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
target_link_libraries(clickhouse-server daemon clickhouse_storages_system clickhouse_functions clickhouse_aggregate_functions)

add_library(clickhouse-local LocalServer.cpp)
target_link_libraries(clickhouse-local dbms clickhouse_functions clickhouse_aggregate_functions)

add_library(clickhouse-extract-from-config ExtractFromConfig.cpp)
target_link_libraries(clickhouse-extract-from-config dbms)

add_executable(clickhouse main.cpp)
target_link_libraries(clickhouse
    clickhouse-server
    clickhouse-client
    clickhouse-local
    clickhouse-benchmark
    clickhouse-extract-from-config)
INSTALL(TARGETS clickhouse RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)

# make symbolic links to concrete clickhouse applications
macro(install_symlink_to_clickhouse app)
    INSTALL(CODE "execute_process(COMMAND ln -sf clickhouse ${app} WORKING_DIRECTORY \"\$ENV{DESTDIR}${CMAKE_INSTALL_FULL_BINDIR}\" )" COMPONENT ${app})
endmacro(install_symlink_to_clickhouse)

install_symlink_to_clickhouse(clickhouse-server)
install_symlink_to_clickhouse(clickhouse-client)
install_symlink_to_clickhouse(clickhouse-local)
install_symlink_to_clickhouse(clickhouse-benchmark)

INSTALL(
    FILES config.xml users.xml
    DESTINATION  ${CLICKHOUSE_ETC_DIR}/clickhouse-server
    COMPONENT clickhouse)
