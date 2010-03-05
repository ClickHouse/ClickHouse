#include <Poco/SharedPtr.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Storages/StorageSystemNumbers.h>


namespace DB
{

using Poco::SharedPtr;


NumbersBlockInputStream::NumbersBlockInputStream(size_t block_size_) : block_size(block_size_), next(0)
{
}


Block NumbersBlockInputStream::read()
{
	Block res;
	res.insert(ColumnWithNameAndType());
	ColumnWithNameAndType & column_with_name_and_type = res.getByPosition(0);
	column_with_name_and_type.name = "number";
	column_with_name_and_type.type = new DataTypeUInt64();
	column_with_name_and_type.column = new Column;
	*column_with_name_and_type.column = UInt64Column(block_size);
	UInt64Column & vec = boost::get<UInt64Column>(*column_with_name_and_type.column);

	for (size_t i = 0; i < block_size; ++i)
		vec[i] = next++;

	return res;
}


SharedPtr<IBlockInputStream> StorageSystemNumbers::read(
	const ColumnNames & column_names, const ptree & query, size_t max_block_size)
{
	if (column_names.size() != 1)
		throw Exception("Incorrect number of columns.", ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS);

	if (column_names[0] != "number")
		throw Exception("There is no column " + column_names[0] + " in table System.Numbers.",
			ErrorCodes::THERE_IS_NO_COLUMN);

	return new NumbersBlockInputStream(max_block_size);
}

}
