#pragma once

#include <string>


namespace DB
{

/** Passing arbitary connection string to ODBC Driver Manager is insecure, for the following reasons:
  * 1. Driver Manager like unixODBC has multiple bugs like buffer overflow.
  * 2. Driver Manager can interpret some parameters as a path to library for dlopen or a file to read,
  *    thus allows arbitary remote code execution.
  *
  * This function will throw exception if connection string has insecure parameters.
  * It may also modify connection string to harden it.
  *
  * Note that it is intended for ANSI (not multibyte) variant of connection string.
  */
std::string validateODBCConnectionString(const std::string & connection_string);

}
