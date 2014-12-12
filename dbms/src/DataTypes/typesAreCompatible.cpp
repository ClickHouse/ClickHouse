#include <DB/DataTypes/typesAreCompatible.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <string>
#include <vector>
#include <map>
#include <algorithm>

namespace
{

typedef std::map<std::string, std::vector<std::string> > TypesCompatibilityList;

void addTypes(TypesCompatibilityList& tcl, const DB::IDataType & lhs, const DB::IDataType& rhs)
{
	tcl[lhs.getName()].push_back(rhs.getName());
	tcl[rhs.getName()].push_back(lhs.getName());
}

TypesCompatibilityList init()
{
	TypesCompatibilityList tcl;

	addTypes(tcl, DB::DataTypeString(), DB::DataTypeFixedString(1));

	addTypes(tcl, DB::DataTypeFloat32(), DB::DataTypeFloat64());

	addTypes(tcl, DB::DataTypeInt8(), DB::DataTypeUInt8());
	addTypes(tcl, DB::DataTypeInt8(), DB::DataTypeInt16());
	addTypes(tcl, DB::DataTypeInt8(), DB::DataTypeUInt16());
	addTypes(tcl, DB::DataTypeInt8(), DB::DataTypeInt32());
	addTypes(tcl, DB::DataTypeInt8(), DB::DataTypeUInt32());
	addTypes(tcl, DB::DataTypeInt8(), DB::DataTypeInt64());
	addTypes(tcl, DB::DataTypeInt8(), DB::DataTypeUInt64());

	addTypes(tcl, DB::DataTypeInt16(), DB::DataTypeUInt16()),
	addTypes(tcl, DB::DataTypeInt16(), DB::DataTypeInt32()),
	addTypes(tcl, DB::DataTypeInt16(), DB::DataTypeUInt32()),
	addTypes(tcl, DB::DataTypeInt16(), DB::DataTypeInt64()),
	addTypes(tcl, DB::DataTypeInt16(), DB::DataTypeUInt64()),

	addTypes(tcl, DB::DataTypeInt32(), DB::DataTypeUInt32()),
	addTypes(tcl, DB::DataTypeInt32(), DB::DataTypeInt64());
	addTypes(tcl, DB::DataTypeInt32(), DB::DataTypeUInt64());

	addTypes(tcl, DB::DataTypeInt64(), DB::DataTypeUInt64());

	for (auto & it : tcl)
	{
		auto & types_list = it.second;
		std::sort(types_list.begin(), types_list.end());
	}
	
	return tcl;
}

}

namespace DB
{

bool typesAreCompatible(const IDataType & lhs, const IDataType & rhs)
{
	static const auto types_compatibility_list = init();
	
	if (lhs.getName() == rhs.getName())
		return true;

	auto it = types_compatibility_list.find(lhs.getName());
	if (it == types_compatibility_list.end())
		return false;
	
	const auto & types_list = it->second;
	return std::binary_search(types_list.begin(), types_list.end(), rhs.getName());
}

}
