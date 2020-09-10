#pragma once
#include <Poco/Exception.h>

template <typename T>
inline void throwError(const T & err)
{
    throw Poco::Exception(err);
}
