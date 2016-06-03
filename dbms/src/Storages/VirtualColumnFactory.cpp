#include <DB/Storages/VirtualColumnFactory.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int NO_SUCH_COLUMN_IN_TABLE;
}


DataTypePtr VirtualColumnFactory::getType(const String & name)
{
	auto res = tryGetType(name);
	if (!res)
		throw Exception("There is no column " + name + " in table.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
	return res;
}

bool VirtualColumnFactory::hasColumn(const String & name)
{
	return !!tryGetType(name);
}

DataTypePtr VirtualColumnFactory::tryGetType(const String & name)
{
	if (name == "_table")			return new DataTypeString;
	if (name == "_part")			return new DataTypeString;
	if (name == "_part_index")		return new DataTypeUInt64;
	if (name == "_sample_factor")	return new DataTypeFloat64;
	if (name == "_replicated")		return new DataTypeUInt8;
	return nullptr;
}

}
