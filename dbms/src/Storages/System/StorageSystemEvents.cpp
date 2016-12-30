#include <DB/Common/ProfileEvents.h>
#include <DB/Common/CurrentMetrics.h>
#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/System/StorageSystemEvents.h>


namespace DB
{


enum class EventType
{
	ProfileEvent = 0,
	CurrentMetric = 1
};


StorageSystemEvents::StorageSystemEvents(const std::string & name_)
	: name(name_),
	columns
	{
		{"type",		std::make_shared<DataTypeInt8>()},
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
			size_t col = 0;
			block.unsafeGetByPosition(col++).column->insert(static_cast<Int64>(EventType::ProfileEvent));
			block.unsafeGetByPosition(col++).column->insert(String(ProfileEvents::getDescription(ProfileEvents::Event(i))));
			block.unsafeGetByPosition(col++).column->insert(value);
		}
	}

	for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
	{
		UInt64 value = CurrentMetrics::values[i];

		if (0 != value)
		{
			size_t col = 0;
			block.unsafeGetByPosition(col++).column->insert(static_cast<Int64>(EventType::CurrentMetric));
			block.unsafeGetByPosition(col++).column->insert(String(CurrentMetrics::getDescription(CurrentMetrics::Metric(i))));
			block.unsafeGetByPosition(col++).column->insert(value);
		}
	}

	return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
