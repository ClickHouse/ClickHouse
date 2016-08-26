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
		{"value",		std::make_shared<DataTypeUInt64>()},
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

	Block block;

	ColumnWithTypeAndName col_event;
	col_event.name = "event";
	col_event.type = std::make_shared<DataTypeString>();
	col_event.column = std::make_shared<ColumnString>();
	block.insert(col_event);

	ColumnWithTypeAndName col_value;
	col_value.name = "value";
	col_value.type = std::make_shared<DataTypeUInt64>();
	col_value.column = std::make_shared<ColumnUInt64>();
	block.insert(col_value);

	for (size_t i = 0; i < ProfileEvents::END; ++i)
	{
		UInt64 value = ProfileEvents::counters[i];

		if (0 != value)
		{
			col_event.column->insert(String(ProfileEvents::getDescription(ProfileEvents::Event(i))));
			col_value.column->insert(value);
		}
	}

	return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
