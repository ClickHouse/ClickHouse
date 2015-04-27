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
		  { "type", new DataTypeString },
		  { "origin", new DataTypeString },
		  { "attribute.names", new DataTypeArray{new DataTypeString} },
		  { "attribute.types", new DataTypeArray{new DataTypeString} },
		  { "has_hierarchy", new DataTypeUInt8 },
		  { "bytes_allocated", new DataTypeUInt64 },
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

	ColumnWithNameAndType col_name{new ColumnString, new DataTypeString, "name"};
	ColumnWithNameAndType col_type{new ColumnString, new DataTypeString, "type"};
	ColumnWithNameAndType col_origin{new ColumnString, new DataTypeString, "origin"};
	ColumnWithNameAndType col_attribute_names{
		new ColumnArray{new ColumnString},
		new DataTypeArray{new DataTypeString},
		"attribute.names"
	};
	ColumnWithNameAndType col_attribute_types{
		new ColumnArray{new ColumnString},
		new DataTypeArray{new DataTypeString},
		"attribute.types"
	};
	ColumnWithNameAndType col_has_hierarchy{new ColumnUInt8, new DataTypeUInt8, "has_hierarchy"};
	ColumnWithNameAndType col_bytes_allocated{new ColumnUInt64, new DataTypeUInt64, "bytes_allocated"};
	ColumnWithNameAndType col_hit_rate{new ColumnFloat64, new DataTypeFloat64, "hit_rate"};
	ColumnWithNameAndType col_element_count{new ColumnUInt64, new DataTypeUInt64, "element_count"};
	ColumnWithNameAndType col_load_factor{new ColumnFloat64, new DataTypeFloat64, "load_factor"};
	ColumnWithNameAndType col_creation_time{new ColumnUInt32, new DataTypeDateTime, "creation_time"};
	ColumnWithNameAndType col_last_exception{new ColumnString, new DataTypeString, "last_exception"};
	ColumnWithNameAndType col_source{new ColumnString, new DataTypeString, "source"};

	const auto & external_dictionaries = context.getExternalDictionaries();
	const std::lock_guard<std::mutex> lock{external_dictionaries.dictionaries_mutex};

	for (const auto & dict_info : external_dictionaries.dictionaries)
	{
		const auto & name = dict_info.first;
		const auto dict_ptr = dict_info.second.first->get();

		col_name.column->insert(name);
		col_type.column->insert(dict_ptr->getTypeName());
		col_origin.column->insert(dict_info.second.second);

		const auto & dict_struct = dict_ptr->getStructure();
		col_attribute_names.column->insert(ext::map<Array>(dict_struct.attributes, [] (auto & attr) -> decltype(auto) {
			return attr.name;
		}));
		col_attribute_types.column->insert(ext::map<Array>(dict_struct.attributes, [] (auto & attr) -> decltype(auto) {
			return attr.type->getName();
		}));
		col_has_hierarchy.column->insert(UInt64{dict_ptr->hasHierarchy()});
		col_bytes_allocated.column->insert(dict_ptr->getBytesAllocated());
		col_hit_rate.column->insert(dict_ptr->getHitRate());
		col_element_count.column->insert(dict_ptr->getElementCount());
		col_load_factor.column->insert(dict_ptr->getLoadFactor());
		col_creation_time.column->insert(std::chrono::system_clock::to_time_t(dict_ptr->getCreationTime()));

		const auto exception_it = external_dictionaries.stored_exceptions.find(name);
		if (exception_it != std::end(external_dictionaries.stored_exceptions))
		{
			try
			{
				std::rethrow_exception(exception_it->second);
			}
			catch (const Exception & e)
			{
				col_last_exception.column->insert("DB::Exception. Code " + toString(e.code()) + ". " + e.message());
			}
			catch (const Poco::Exception & e)
			{
				col_last_exception.column->insert("Poco::Exception. " + e.message());
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
			col_last_exception.column->insert(std::string{});

		col_source.column->insert(dict_ptr->getSource()->toString());
	}

	Block block{
		col_name,
		col_type,
		col_origin,
		col_attribute_names,
		col_attribute_types,
		col_has_hierarchy,
		col_bytes_allocated,
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
