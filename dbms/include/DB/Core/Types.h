#ifndef DBMS_CORE_TYPES_H
#define DBMS_CORE_TYPES_H

#include <string>
#include <boost/none.hpp>
#include <Poco/Types.h>
#include <Poco/SharedPtr.h>


namespace DB
{

/** Типы данных для представления значений из БД в оперативке.
  */

typedef boost::none_t Null;

typedef Poco::UInt8 UInt8;
typedef Poco::UInt16 UInt16;
typedef Poco::UInt32 UInt32;
typedef Poco::UInt64 UInt64;

typedef Poco::Int8 Int8;
typedef Poco::Int16 Int16;
typedef Poco::Int32 Int32;
typedef Poco::Int64 Int64;

typedef float Float32;
typedef double Float64;

typedef std::string String;

}

#endif
