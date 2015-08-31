#include <DB/Common/ProfileEvents.h>
#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/StorageSystemEvents.h>


namespace DB
{


StorageSystemEvents::StorageSystemEvents(const std::string & name_)
	: name(name_),
	columns
	{
		{"event", 		new DataTypeString},
		{"value",		new DataTypeUInt64},
	}
{
}

StoragePtr StorageSystemEvents::create(const std::string & name_)
{
	return (new StorageSystemEvents(name_))->thisPtr();
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
	col_event.type = new DataTypeString;
	col_event.column = new ColumnString;
	block.insert(col_event);

	ColumnWithTypeAndName col_value;
	col_value.name = "value";
	col_value.type = new DataTypeUInt64;
	col_value.column = new ColumnUInt64;
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

	return BlockInputStreams(1, new OneBlockInputStream(block));
}


}
