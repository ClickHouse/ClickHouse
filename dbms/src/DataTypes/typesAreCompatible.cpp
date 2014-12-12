#include <DB/DataTypes/typesAreCompatible.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <string>
#include <vector>
#include <utility>
#include <algorithm>

namespace
{

#define REGISTER_COMPATIBLE_TYPES(T, U)     \
	{ T.getName(), U.getName() },           \
	{ U.getName(), T.getName() }            \

std::vector<std::pair<std::string, std::string> > init()
{
	std::vector<std::pair<std::string, std::string> > types_desc =
	{
	    REGISTER_COMPATIBLE_TYPES(DB::DataTypeString(), DB::DataTypeFixedString(1)),
	    
	    REGISTER_COMPATIBLE_TYPES(DB::DataTypeFloat32(), DB::DataTypeFloat64()),

		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt8(), DB::DataTypeUInt8()),
		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt8(), DB::DataTypeInt16()),
		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt8(), DB::DataTypeUInt16()),
		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt8(), DB::DataTypeInt32()),
		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt8(), DB::DataTypeUInt32()),
		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt8(), DB::DataTypeInt64()),
		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt8(), DB::DataTypeUInt64()),

		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt16(), DB::DataTypeUInt16()),
		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt16(), DB::DataTypeInt32()),
		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt16(), DB::DataTypeUInt32()),
		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt16(), DB::DataTypeInt64()),
		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt16(), DB::DataTypeUInt64()),

		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt32(), DB::DataTypeUInt32()),
		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt32(), DB::DataTypeInt64()),
		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt32(), DB::DataTypeUInt64()),

		REGISTER_COMPATIBLE_TYPES(DB::DataTypeInt64(), DB::DataTypeUInt64())
	};
	std::sort(types_desc.begin(), types_desc.end());
	return types_desc;
}

}

namespace DB
{

bool typesAreCompatible(const IDataType & lhs, const IDataType & rhs)
{
	static const auto types_desc = init();
	
	if (lhs.getName() == rhs.getName())
		return true;

	return std::binary_search(types_desc.begin(), types_desc.end(), std::make_pair(lhs.getName(), rhs.getName()));
}

}
