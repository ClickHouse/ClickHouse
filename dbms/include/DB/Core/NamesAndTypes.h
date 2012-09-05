#pragma once

#include <map>
#include <list>
#include <string>

#include <Poco/SharedPtr.h>

#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;

typedef std::pair<std::string, DataTypePtr> NameAndTypePair;
typedef std::list<NameAndTypePair> NamesAndTypesList;
typedef SharedPtr<NamesAndTypesList> NamesAndTypesListPtr;

typedef std::map<std::string, DataTypePtr> NamesAndTypesMap;
typedef SharedPtr<NamesAndTypesMap> NamesAndTypesMapPtr;

}
