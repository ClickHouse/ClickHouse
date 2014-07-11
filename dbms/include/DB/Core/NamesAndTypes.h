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

	NameAndTypePair() {}
	NameAndTypePair(const String & name_, const DataTypePtr & type_) : name(name_), type(type_) {}

	bool operator<(const NameAndTypePair & rhs) const
	{
		return std::forward_as_tuple(name, type->getName()) < std::forward_as_tuple(rhs.name, rhs.type->getName());
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
