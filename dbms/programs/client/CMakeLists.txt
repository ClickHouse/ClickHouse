
# wget https://raw.githubusercontent.com/openssh/openssh-portable/master/openbsd-compat/readpassphrase.c -O readpassphrase.cpp
# wget https://raw.githubusercontent.com/openssh/openssh-portable/master/openbsd-compat/readpassphrase.h

set(CLICKHOUSE_CLIENT_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/Client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ConnectionParameters.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/readpassphrase.cpp
)

set(CLICKHOUSE_CLIENT_LINK PRIVATE clickhouse_common_config clickhouse_functions clickhouse_aggregate_functions clickhouse_common_io ${LINE_EDITING_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY})
set(CLICKHOUSE_CLIENT_INCLUDE SYSTEM PRIVATE ${READLINE_INCLUDE_DIR})

include(CheckSymbolExists)
check_symbol_exists(readpassphrase readpassphrase.h HAVE_READPASSPHRASE)
configure_file(includes.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/includes.h)
if (NOT HAVE_READPASSPHRASE)
    list(APPEND CLICKHOUSE_CLIENT_INCLUDE ${CLICKHOUSE_CLIENT_INCLUDE} PRIVATE . ${CMAKE_CURRENT_BINARY_DIR}/include/)
endif()

clickhouse_program_add(client)

install (FILES clickhouse-client.xml DESTINATION ${CLICKHOUSE_ETC_DIR}/clickhouse-client COMPONENT clickhouse-client RENAME config.xml)
