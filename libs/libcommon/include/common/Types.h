#pragma once

#include <Poco/Types.h>

using Int8 = Poco::Int8;
using Int16 = Poco::Int16;
using Int32 = Poco::Int32;
using Int64 = Poco::Int64;

using UInt8 = Poco::UInt8;
using UInt16 = Poco::UInt16;
using UInt32 = Poco::UInt32;
using UInt64 = Poco::UInt64;


/// Обход проблемы с тем, что KDevelop не видит time_t и size_t (для подсветки синтаксиса).
#ifdef IN_KDEVELOP_PARSER
    using time_t = Int64;
    using size_t = UInt64;
#endif
