#include <DB/Storages/StorageSystemDictionaries.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Columns/ColumnString.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/IDictionary.h>
#include <mutex>

namespace DB
{

StorageSystemDictionaries::StorageSystemDictionaries(const std::string & name)
	: name{name},
	  columns{
		{ "name", new DataTypeString },
		{ "type", new DataTypeString },
		{ "origin", new DataTypeString },
	}
{
}

StoragePtr StorageSystemDictionaries::create(const std::string & name)
{
	return (new StorageSystemDictionaries{name})->thisPtr();
}

BlockInputStreams StorageSystemDictionaries::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	ColumnWithNameAndType col_name{new ColumnString, new DataTypeString, "name"};
	ColumnWithNameAndType col_type{new ColumnString, new DataTypeString, "type"};
	ColumnWithNameAndType col_origin{new ColumnString, new DataTypeString, "origin"};

	const auto & external_dictionaries = context.getExternalDictionaries();
	const std::lock_guard<std::mutex> lock{external_dictionaries.dictionaries_mutex};

	for (const auto & dict_info : external_dictionaries.dictionaries)
	{
		col_name.column->insert(dict_info.first);
		col_type.column->insert(dict_info.second.first->get()->getTypeName());
		col_origin.column->insert(dict_info.second.second);
	}

	Block block{
		col_name,
		col_type,
		col_origin
	};

	return BlockInputStreams{1, new OneBlockInputStream{block}};
}

}
