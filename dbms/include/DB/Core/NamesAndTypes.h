#ifndef DBMS_CORE_NAMES_AND_TYPES_H
#define DBMS_CORE_NAMES_AND_TYPES_H

#include <map>
#include <string>

#include <Poco/SharedPtr.h>

#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;

typedef std::map<std::string, DataTypePtr> NamesAndTypes;

}

#endif
