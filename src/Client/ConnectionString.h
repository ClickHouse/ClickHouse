#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace DB
{
/** Tries to parse ClickHouse connection string.
 * if @connection_string starts with 'clickhouse:' then connection string will be parsed
 * and converted into a set of arguments for the client.
 * Connection string format is similar to URI "clickhouse:[//[user[:password]@][hosts_and_ports]][/dbname][?query_parameters]"
 * with the difference that hosts_and_ports can contain multiple hosts separated by ','.
 * example: clickhouse://user@host1:port1,host2:port2
 * @return Returns false if no connection string was specified. If a connection string was specified, returns true if it is valid, and throws an exception if it is invalid.
 * @exception Throws DB::Exception if URI has valid scheme (clickhouse:), but invalid internals.
*/
bool tryParseConnectionString(
    std::string_view connection_string,
    std::vector<std::string> & common_arguments,
    std::vector<std::vector<std::string>> & hosts_and_ports_arguments);

// Throws DB::Exception with BAD_ARGUMENTS if the given command line argument
// is not allowed to be used with a connection string.
void checkIfCmdLineOptionCanBeUsedWithConnectionString(std::string_view command_line_option);

}
