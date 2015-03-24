#include <DB/Storages/StorageSystemDictionaries.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/IDictionary.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <mutex>

namespace DB
{

StorageSystemDictionaries::StorageSystemDictionaries(const std::string & name)
	: name{name},
	  columns{
		  { "name", new DataTypeString },
		  { "type", new DataTypeString },
		  { "origin", new DataTypeString },
		  { "attribute_names", new DataTypeArray{new DataTypeString} },
		  { "attribute_types", new DataTypeArray{new DataTypeString} },
		  { "bytes_allocated", new DataTypeUInt64 },
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
	ColumnWithNameAndType col_attribute_names{
		new ColumnArray{new ColumnString},
		new DataTypeArray{new DataTypeString},
		"attribute_names"
	};
	ColumnWithNameAndType col_attribute_types{
		new ColumnArray{new ColumnString},
		new DataTypeArray{new DataTypeString},
		"attribute_types"
	};
	ColumnWithNameAndType col_bytes_allocated{new ColumnUInt64, new DataTypeUInt64, "bytes_allocated"};

	const auto & external_dictionaries = context.getExternalDictionaries();
	const std::lock_guard<std::mutex> lock{external_dictionaries.dictionaries_mutex};

	for (const auto & dict_info : external_dictionaries.dictionaries)
	{
		const auto dict_ptr = dict_info.second.first->get();

		col_name.column->insert(dict_info.first);
		col_type.column->insert(dict_ptr->getTypeName());
		col_origin.column->insert(dict_info.second.second);

		const auto & dict_struct = dict_ptr->getStructure();
		Array attribute_names;
		Array attribute_types;
		for (const auto & attribute : dict_struct.attributes)
		{
			attribute_names.push_back(attribute.name);
			attribute_types.push_back(attribute.type->getName());
		}
		col_attribute_names.column->insert(attribute_names);
		col_attribute_types.column->insert(attribute_types);
		col_bytes_allocated.column->insert(dict_ptr->getBytesAllocated());
	}

	Block block{
		col_name,
		col_type,
		col_origin,
		col_attribute_names,
		col_attribute_types,
		col_bytes_allocated
	};

	return BlockInputStreams{1, new OneBlockInputStream{block}};
}

}
