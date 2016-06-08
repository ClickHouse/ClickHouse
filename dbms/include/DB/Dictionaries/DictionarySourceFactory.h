#pragma once

#include <DB/Core/Block.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Dictionaries/FileDictionarySource.h>
#include <DB/Dictionaries/MySQLDictionarySource.h>
#include <DB/Dictionaries/ClickHouseDictionarySource.h>

#ifndef DISABLE_MONGODB
#include <DB/Dictionaries/MongoDBDictionarySource.h>
#endif

#include <DB/Dictionaries/ODBCDictionarySource.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Core/FieldVisitors.h>
#include <common/singleton.h>
#include <memory>
#include <Poco/Data/ODBC/Connector.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_ELEMENT_IN_CONFIG;
	extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
	extern const int LOGICAL_ERROR;
	extern const int SUPPORT_IS_DISABLED;
}

namespace
{

Block createSampleBlock(const DictionaryStructure & dict_struct)
{
	Block block;

	if (dict_struct.id)
		block.insert(ColumnWithTypeAndName{
			std::make_shared<ColumnUInt64>(1), std::make_shared<DataTypeUInt64>(), dict_struct.id.value().name});

	if (dict_struct.key)
	{
		for (const auto & attribute : *dict_struct.key)
		{
			auto column = attribute.type->createColumn();
			column->insertDefault();

			block.insert(ColumnWithTypeAndName{column, attribute.type, attribute.name});
		}
	}

	if (dict_struct.range_min)
		for (const auto & attribute : { dict_struct.range_min, dict_struct.range_max })
			block.insert(ColumnWithTypeAndName{
				std::make_shared<ColumnUInt16>(1), std::make_shared<DataTypeDate>(), attribute.value().name});

	for (const auto & attribute : dict_struct.attributes)
	{
		auto column = attribute.type->createColumn();
		column->insert(attribute.null_value);

		block.insert(ColumnWithTypeAndName{column, attribute.type, attribute.name});
	}

	return block;
}

}

/// creates IDictionarySource instance from config and DictionaryStructure
class DictionarySourceFactory : public Singleton<DictionarySourceFactory>
{
public:
    DictionarySourceFactory()
	{
		Poco::Data::ODBC::Connector::registerConnector();
	}

	DictionarySourcePtr create(
		const std::string & name, Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
		const DictionaryStructure & dict_struct, Context & context) const
	{
		Poco::Util::AbstractConfiguration::Keys keys;
		config.keys(config_prefix, keys);
		if (keys.size() != 1)
			throw Exception{
				name +": element dictionary.source should have exactly one child element",
				ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG
			};

		auto sample_block = createSampleBlock(dict_struct);

		const auto & source_type = keys.front();

		if ("file" == source_type)
		{
			if (dict_struct.has_expressions)
				throw Exception{
					"Dictionary source of type `file` does not support attribute expressions",
					ErrorCodes::LOGICAL_ERROR};

			const auto filename = config.getString(config_prefix + ".file.path");
			const auto format = config.getString(config_prefix + ".file.format");
			return std::make_unique<FileDictionarySource>(filename, format, sample_block, context);
		}
		else if ("mysql" == source_type)
		{
			return std::make_unique<MySQLDictionarySource>(dict_struct, config, config_prefix + ".mysql", sample_block);
		}
		else if ("clickhouse" == source_type)
		{
			return std::make_unique<ClickHouseDictionarySource>(dict_struct, config, config_prefix + ".clickhouse",
				sample_block, context);
		}
		else if ("mongodb" == source_type)
		{
		#ifndef DISABLE_MONGODB
			return std::make_unique<MongoDBDictionarySource>(dict_struct, config, config_prefix + ".mongodb", sample_block);
		#else
			throw Exception{
				"MongoDB dictionary source was disabled at build time", ErrorCodes::SUPPORT_IS_DISABLED};
		#endif
		}
		else if ("odbc" == source_type)
		{
			return std::make_unique<ODBCDictionarySource>(dict_struct, config, config_prefix + ".odbc", sample_block);
		}

		throw Exception{
			name + ": unknown dictionary source type: " + source_type,
			ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG};
	}
};

}
