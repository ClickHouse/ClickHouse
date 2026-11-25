#pragma once

#include <string>
#include <map>
#include <base/types.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

/** Parse address from string, that can contain host with or without port.
  * If port was not specified and default_port is not zero, default_port is used.
  * Otherwise, an exception is thrown.
  *
  * Examples:
  *  clickhouse.com - returns "clickhouse.com" and default_port
  *  clickhouse.com:80 - returns "clickhouse.com" and 80
  *  [2a02:6b8:a::a]:80 - returns [2a02:6b8:a::a] and 80; note that square brackets remain in returned host.
  */
std::pair<std::string, UInt16> parseAddress(const std::string & str, UInt16 default_port);

/** Read a host name or an IP address from the config key `key`.
  * Throw `INVALID_CONFIG_PARAMETER`, naming the key and the value, when the value cannot be a host:
  * it is empty, contains whitespace or a control character (a typo such as `<host> 10.0.0.1</host>`),
  * or consists of digits and dots only but is not an IPv4 address (`10.0.0.256`).
  * Anything else is left to name resolution, which is not attempted here: a host may not resolve yet at startup.
  */
std::string getHostFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & key);

}
