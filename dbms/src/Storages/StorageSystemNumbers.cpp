#include <Poco/SharedPtr.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Storages/StorageSystemNumbers.h>


namespace DB
{

using Poco::SharedPtr;


NumbersBlockInputStream::NumbersBlockInputStream(size_t block_size_) : block_size(block_size_), next(0)
{
}


Block NumbersBlockInputStream::readImpl()
{
	Block res;
	
	ColumnWithNameAndType column_with_name_and_type;
	
	column_with_name_and_type.name = "number";
	column_with_name_and_type.type = new DataTypeUInt64();
	ColumnUInt64 * column = new ColumnUInt64(block_size);
	ColumnUInt64::Container_t & vec = column->getData();
	column_with_name_and_type.column = column;

	for (size_t i = 0; i < block_size; ++i)
		vec[i] = next++;

	res.insert(column_with_name_and_type);
	
	return res;
}


StorageSystemNumbers::StorageSystemNumbers(const std::string & name_)
	: name(name_)
{
	columns.push_back(NameAndTypePair("number", new DataTypeUInt64));
}

StoragePtr StorageSystemNumbers::create(const std::string & name_)
{
	return (new StorageSystemNumbers(name_))->thisPtr();
}


BlockInputStreams StorageSystemNumbers::read(
	const Names & column_names, ASTPtr query, const Settings & settings,
	QueryProcessingStage::Enum & processed_stage, size_t max_block_size, unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;
	return BlockInputStreams(1, new NumbersBlockInputStream(max_block_size));
}

}
