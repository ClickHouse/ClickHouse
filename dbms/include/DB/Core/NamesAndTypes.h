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
typedef std::vector<NameAndTypePair> NamesAndTypes;

inline std::string formatColumnsForCreateQuery(NamesAndTypesList & columns)
{
	std::string res;
	res += "(";
	for (NamesAndTypesList::iterator it = columns.begin(); it != columns.end(); ++it)
	{
		if (it != columns.begin())
			res += ", ";
		res += it->first;
		res += " ";
		res += it->second->getName();
	}
	res += ")";
	return res;
}


}
