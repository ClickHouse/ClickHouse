#include <DB/Common/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/System/StorageSystemOne.h>


namespace DB
{


StorageSystemOne::StorageSystemOne(const std::string & name_)
	: name(name_), columns{{"dummy", new DataTypeUInt8}}
{
}

StoragePtr StorageSystemOne::create(const std::string & name_)
{
	return (new StorageSystemOne(name_))->thisPtr();
}


BlockInputStreams StorageSystemOne::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	Block block;
	ColumnWithTypeAndName col;
	col.name = "dummy";
	col.type = new DataTypeUInt8;
	col.column = ColumnConstUInt8(1, 0).convertToFullColumn();
	block.insert(col);

	return BlockInputStreams(1, new OneBlockInputStream(block));
}


}
