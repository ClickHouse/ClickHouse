#include <DB/Common/ProfileEvents.h>
#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/System/StorageSystemEvents.h>


namespace DB
{


StorageSystemEvents::StorageSystemEvents(const std::string & name_)
	: name(name_),
	columns
	{
		{"event", 		std::make_shared<DataTypeString>()},
		{"value",		std::make_shared<DataTypeUInt64>()}
	}
{
}

StoragePtr StorageSystemEvents::create(const std::string & name_)
{
	return make_shared(name_);
}


BlockInputStreams StorageSystemEvents::read(
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

	Block block = getSampleBlock();

	for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
	{
		UInt64 value = ProfileEvents::counters[i];

		if (0 != value)
		{
			block.unsafeGetByPosition(0).column->insert(String(ProfileEvents::getDescription(ProfileEvents::Event(i))));
			block.unsafeGetByPosition(1).column->insert(value);
		}
	}

	return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
