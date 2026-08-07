#pragma once

#include <string>

/** Get path to the running executable if possible.
  * It is possible when:
  * - procfs exists;
  * - there is a /proc/self/exe file;
  * Otherwise return empty string.
  */
std::string getExecutablePath();
