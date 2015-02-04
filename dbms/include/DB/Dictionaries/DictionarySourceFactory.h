#pragma once

#include <DB/Core/Block.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Dictionaries/FileDictionarySource.h>
#include <DB/Dictionaries/MysqlDictionarySource.h>
#include <DB/Dictionaries/ClickhouseDictionarySource.h>
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

class DictionarySourceFactory : public Singleton<DictionarySourceFactory>
{
public:
	DictionarySourcePtr create(Poco::Util::AbstractConfiguration & config,
		const std::string & config_prefix,
		const DictionaryStructure & dict_struct,
		Context & context) const
	{
		auto sample_block = createSampleBlock(dict_struct, context);

		if (config.has(config_prefix + "file"))
		{
			const auto & filename = config.getString(config_prefix + "file.path");
			const auto & format = config.getString(config_prefix + "file.format");
			return ext::make_unique<FileDictionarySource>(filename, format, sample_block, context);
		}
		else if (config.has(config_prefix + "mysql"))
		{
			return ext::make_unique<MysqlDictionarySource>(config, config_prefix + "mysql.", sample_block, context);
		}
		else if (config.has(config_prefix + "clickhouse"))
		{
			ext::make_unique<ClickhouseDictionarySource>(config, config_prefix + "clickhouse.",
				sample_block, context);
		}

		throw Exception{"unsupported source type"};
	}
};

}
