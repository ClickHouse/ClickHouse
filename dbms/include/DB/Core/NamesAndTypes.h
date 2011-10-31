#pragma once

#include <map>
#include <string>

#include <Poco/SharedPtr.h>

#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;

typedef std::map<std::string, DataTypePtr> NamesAndTypes;
typedef SharedPtr<NamesAndTypes> NamesAndTypesPtr;

}
