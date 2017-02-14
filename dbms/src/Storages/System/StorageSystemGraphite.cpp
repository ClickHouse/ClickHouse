#include <DB/Storages/System/StorageSystemGraphite.h>

#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

namespace DB
{

StorageSystemGraphite::StorageSystemGraphite(const std::string & name_)
	: name(name_)
	, columns {
		{"metric", std::make_shared<DataTypeString>()},
		{"value",  std::make_shared<DataTypeInt64>()}}
{
}

StoragePtr StorageSystemGraphite::create(const std::string & name_)
{
	return nullptr;
}

BlockInputStreams StorageSystemGraphite::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	return BlockInputStreams();
}

}
