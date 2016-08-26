#include <DB/Storages/System/StorageSystemDictionaries.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/IDictionary.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Interpreters/ExternalDictionaries.h>
#include <ext/map.hpp>
#include <mutex>

namespace DB
{

StorageSystemDictionaries::StorageSystemDictionaries(const std::string & name)
	: name{name},
	  columns{
		  { "name", std::make_shared<DataTypeString>() },
		  { "origin", std::make_shared<DataTypeString>() },
		  { "type", std::make_shared<DataTypeString>() },
		  { "key", std::make_shared<DataTypeString>() },
		  { "attribute.names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
		  { "attribute.types", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
		  { "bytes_allocated", std::make_shared<DataTypeUInt64>() },
		  { "query_count", std::make_shared<DataTypeUInt64>() },
		  { "hit_rate", std::make_shared<DataTypeFloat64>() },
		  { "element_count", std::make_shared<DataTypeUInt64>() },
		  { "load_factor", std::make_shared<DataTypeFloat64>() },
		  { "creation_time", std::make_shared<DataTypeDateTime>() },
		  { "last_exception", std::make_shared<DataTypeString>() },
		  { "source", std::make_shared<DataTypeString>() }
	}
{
}

StoragePtr StorageSystemDictionaries::create(const std::string & name)
{
	return make_shared(name);
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

	ColumnWithTypeAndName col_name{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "name"};
	ColumnWithTypeAndName col_origin{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "origin"};
	ColumnWithTypeAndName col_type{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "type"};
	ColumnWithTypeAndName col_key{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "key"};
	ColumnWithTypeAndName col_attribute_names{
		std::make_shared<ColumnArray>(std::make_shared<ColumnString>()),
		std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
		"attribute.names"
	};
	ColumnWithTypeAndName col_attribute_types{
		std::make_shared<ColumnArray>(std::make_shared<ColumnString>()),
		std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
		"attribute.types"
	};
	ColumnWithTypeAndName col_has_hierarchy{std::make_shared<ColumnUInt8>(), std::make_shared<DataTypeUInt8>(), "has_hierarchy"};
	ColumnWithTypeAndName col_bytes_allocated{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "bytes_allocated"};
	ColumnWithTypeAndName col_query_count{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "query_count"};
	ColumnWithTypeAndName col_hit_rate{std::make_shared<ColumnFloat64>(), std::make_shared<DataTypeFloat64>(), "hit_rate"};
	ColumnWithTypeAndName col_element_count{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "element_count"};
	ColumnWithTypeAndName col_load_factor{std::make_shared<ColumnFloat64>(), std::make_shared<DataTypeFloat64>(), "load_factor"};
	ColumnWithTypeAndName col_creation_time{std::make_shared<ColumnUInt32>(), std::make_shared<DataTypeDateTime>(), "creation_time"};
	ColumnWithTypeAndName col_last_exception{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "last_exception"};
	ColumnWithTypeAndName col_source{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "source"};

	const auto & external_dictionaries = context.getExternalDictionaries();
	const std::lock_guard<std::mutex> lock{external_dictionaries.dictionaries_mutex};

	for (const auto & dict_info : external_dictionaries.dictionaries)
	{
		col_name.column->insert(dict_info.first);
		col_origin.column->insert(dict_info.second.origin);

		if (dict_info.second.dict)
		{
			const auto dict_ptr = dict_info.second.dict->get();

			col_type.column->insert(dict_ptr->getTypeName());

			const auto & dict_struct = dict_ptr->getStructure();
			col_key.column->insert(dict_struct.getKeyDescription());

			col_attribute_names.column->insert(ext::map<Array>(dict_struct.attributes, [] (auto & attr) -> decltype(auto) {
				return attr.name;
			}));
			col_attribute_types.column->insert(ext::map<Array>(dict_struct.attributes, [] (auto & attr) -> decltype(auto) {
				return attr.type->getName();
			}));
			col_bytes_allocated.column->insert(dict_ptr->getBytesAllocated());
			col_query_count.column->insert(dict_ptr->getQueryCount());
			col_hit_rate.column->insert(dict_ptr->getHitRate());
			col_element_count.column->insert(dict_ptr->getElementCount());
			col_load_factor.column->insert(dict_ptr->getLoadFactor());
			col_creation_time.column->insert(std::chrono::system_clock::to_time_t(dict_ptr->getCreationTime()));
			col_source.column->insert(dict_ptr->getSource()->toString());
		}
		else
		{
			col_type.column->insertDefault();
			col_key.column->insertDefault();
			col_attribute_names.column->insertDefault();
			col_attribute_types.column->insertDefault();
			col_bytes_allocated.column->insertDefault();
			col_query_count.column->insertDefault();
			col_hit_rate.column->insertDefault();
			col_element_count.column->insertDefault();
			col_load_factor.column->insertDefault();
			col_creation_time.column->insertDefault();
			col_source.column->insertDefault();
		}

		if (dict_info.second.exception)
		{
			try
			{
				std::rethrow_exception(dict_info.second.exception);
			}
			catch (...)
			{
				col_last_exception.column->insert(getCurrentExceptionMessage(false));
			}
		}
		else
			col_last_exception.column->insertDefault();
	}

	Block block{
		col_name,
		col_origin,
		col_type,
		col_key,
		col_attribute_names,
		col_attribute_types,
		col_bytes_allocated,
		col_query_count,
		col_hit_rate,
		col_element_count,
		col_load_factor,
		col_creation_time,
		col_last_exception,
		col_source
	};

	return BlockInputStreams{1, std::make_shared<OneBlockInputStream>(block)};
}

}
