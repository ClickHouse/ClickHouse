#pragma once

#include <string>
#include <map>
#include <common/Types.h>


namespace DB
{

/** Parse address from string, that can contain host with or without port.
  * If port was not specified and default_port is not zero, default_port is used.
  * Otherwise, an exception is thrown.
  *
  * Examples:
  *  yandex.ru - returns "yandex.ru" and default_port
  *  yandex.ru:80 - returns "yandex.ru" and 80
  *  [2a02:6b8:a::a]:80 - returns [2a02:6b8:a::a] and 80; note that square brackets remain in returned host.
  */
std::pair<std::string, UInt16> parseAddress(const std::string & str, UInt16 default_port);

}
