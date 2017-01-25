#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Interpreters/Settings.h>
#include <DB/Storages/System/StorageSystemBuildOptions.h>
#include <common/config_build.h>

namespace DB
{


StorageSystemBuildOptions::StorageSystemBuildOptions(const std::string & name_)
	: name(name_)
	, columns{
		{ "name", 			std::make_shared<DataTypeString>()	},
		{ "value",			std::make_shared<DataTypeString>()	},
	}
{
}

StoragePtr StorageSystemBuildOptions::create(const std::string & name_)
{
	return make_shared(name_);
}


BlockInputStreams StorageSystemBuildOptions::read(
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

	ColumnWithTypeAndName col_name{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "name"};
	ColumnWithTypeAndName col_value{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "value"};

	for (const auto & option : auto_config_build)
	{
		col_name.column->insert(String(option.first));
		col_value.column->insert(String(option.second));
	}

	Block block{
		col_name,
		col_value,
	};

	return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
