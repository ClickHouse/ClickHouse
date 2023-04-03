#pragma once

#include <cerrno>
#include <string>

std::string errnoToString(int code, int the_errno = errno);
