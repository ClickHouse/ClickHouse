#include <DB/Storages/StorageSystemDictionaries.h>
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
#include <statdaemons/ext/map.hpp>
#include <mutex>

namespace DB
{

StorageSystemDictionaries::StorageSystemDictionaries(const std::string & name)
	: name{name},
	  columns{
		  { "name", new DataTypeString },
		  { "origin", new DataTypeString },
		  { "type", new DataTypeString },
		  { "attribute.names", new DataTypeArray{new DataTypeString} },
		  { "attribute.types", new DataTypeArray{new DataTypeString} },
		  { "bytes_allocated", new DataTypeUInt64 },
		  { "query_count", new DataTypeUInt64 },
		  { "hit_rate", new DataTypeFloat64 },
		  { "element_count", new DataTypeUInt64 },
		  { "load_factor", new DataTypeFloat64 },
		  { "creation_time", new DataTypeDateTime },
		  { "last_exception", new DataTypeString },
		  { "source", new DataTypeString }
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

	ColumnWithTypeAndName col_name{new ColumnString, new DataTypeString, "name"};
	ColumnWithTypeAndName col_origin{new ColumnString, new DataTypeString, "origin"};
	ColumnWithTypeAndName col_type{new ColumnString, new DataTypeString, "type"};
	ColumnWithTypeAndName col_attribute_names{
		new ColumnArray{new ColumnString},
		new DataTypeArray{new DataTypeString},
		"attribute.names"
	};
	ColumnWithTypeAndName col_attribute_types{
		new ColumnArray{new ColumnString},
		new DataTypeArray{new DataTypeString},
		"attribute.types"
	};
	ColumnWithTypeAndName col_has_hierarchy{new ColumnUInt8, new DataTypeUInt8, "has_hierarchy"};
	ColumnWithTypeAndName col_bytes_allocated{new ColumnUInt64, new DataTypeUInt64, "bytes_allocated"};
	ColumnWithTypeAndName col_query_count{new ColumnUInt64, new DataTypeUInt64, "query_count"};
	ColumnWithTypeAndName col_hit_rate{new ColumnFloat64, new DataTypeFloat64, "hit_rate"};
	ColumnWithTypeAndName col_element_count{new ColumnUInt64, new DataTypeUInt64, "element_count"};
	ColumnWithTypeAndName col_load_factor{new ColumnFloat64, new DataTypeFloat64, "load_factor"};
	ColumnWithTypeAndName col_creation_time{new ColumnUInt32, new DataTypeDateTime, "creation_time"};
	ColumnWithTypeAndName col_last_exception{new ColumnString, new DataTypeString, "last_exception"};
	ColumnWithTypeAndName col_source{new ColumnString, new DataTypeString, "source"};

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
			catch (const Exception & e)
			{
				col_last_exception.column->insert("DB::Exception. Code " + toString(e.code()) + ". " +
													  std::string{e.displayText()});
			}
			catch (const Poco::Exception & e)
			{
				col_last_exception.column->insert("Poco::Exception. " + std::string{e.displayText()});
			}
			catch (const std::exception & e)
			{
				col_last_exception.column->insert("std::exception. " + std::string{e.what()});
			}
			catch (...)
			{
				col_last_exception.column->insert(std::string{"<unknown exception type>"});
			}
		}
		else
			col_last_exception.column->insertDefault();
	}

	Block block{
		col_name,
		col_origin,
		col_type,
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

	return BlockInputStreams{1, new OneBlockInputStream{block}};
}

}
