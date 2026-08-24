#pragma once

#include <string>


/** Get the FQDN for the local server by resolving DNS hostname - similar to calling the 'hostname' tool with the -f flag.
  * If it does not work, return hostname - similar to calling 'hostname' without flags or 'uname -n'.
  */
const std::string & getFQDNOrHostName();
