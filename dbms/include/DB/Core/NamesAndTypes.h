#pragma once

#include <map>
#include <list>
#include <string>

#include <Poco/SharedPtr.h>

#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;

struct NameAndTypePair
{
	String name;
	DataTypePtr type;

	bool operator<(const NameAndTypePair & rhs) const
	{
		return std::make_pair(name, type->getName()) < std::make_pair(rhs.name, rhs.type->getName());
	}

	bool operator==(const NameAndTypePair & rhs) const
	{
		return name == rhs.name && type->getName() == rhs.type->getName();
	}
};

typedef std::list<NameAndTypePair> NamesAndTypesList;
typedef SharedPtr<NamesAndTypesList> NamesAndTypesListPtr;
typedef std::vector<NameAndTypePair> NamesAndTypes;

}
