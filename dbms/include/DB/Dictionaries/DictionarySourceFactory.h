#pragma once

#include <DB/Core/Block.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Dictionaries/FileDictionarySource.h>
#include <DB/Dictionaries/MySQLDictionarySource.h>
#include <DB/Dictionaries/ClickHouseDictionarySource.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <Yandex/singleton.h>
#include <statdaemons/ext/memory.hpp>

namespace DB
{

namespace
{

Block createSampleBlock(const DictionaryStructure & dict_struct, const Context & context)
{
	Block block{
		ColumnWithNameAndType{
			new ColumnUInt64,
			new DataTypeUInt64,
			dict_struct.id_name
		}
	};

	for (const auto & attribute : dict_struct.attributes)
	{
		const auto & type = context.getDataTypeFactory().get(attribute.type);
		block.insert(ColumnWithNameAndType{
			type->createColumn(), type, attribute.name
		});
	}

	return block;
}

}

/// creates IDictionarySource instance from config and DictionaryStructure
class DictionarySourceFactory : public Singleton<DictionarySourceFactory>
{
public:
	DictionarySourcePtr create(Poco::Util::AbstractConfiguration & config,
		const std::string & config_prefix,
		const DictionaryStructure & dict_struct,
		Context & context) const
	{
		Poco::Util::AbstractConfiguration::Keys keys;
		config.keys(config_prefix, keys);
		if (keys.size() != 1)
			throw Exception{
				"Element dictionary.source should have exactly one child element",
				ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG
			};

		auto sample_block = createSampleBlock(dict_struct, context);

		const auto & source_type = keys.front();

		if ("file" == source_type)
		{
			const auto filename = config.getString(config_prefix + ".file.path");
			const auto format = config.getString(config_prefix + ".file.format");
			return ext::make_unique<FileDictionarySource>(filename, format, sample_block, context);
		}
		else if ("mysql" == source_type)
		{
			return ext::make_unique<MySQLDictionarySource>(config, config_prefix + ".mysql", sample_block, context);
		}
		else if ("clickhouse" == source_type)
		{
			return ext::make_unique<ClickHouseDictionarySource>(config, config_prefix + ".clickhouse",
				sample_block, context);
		}

		throw Exception{
			"Unknown dictionary source type: " + source_type,
			ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG
		};
	}
};

}
