#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/config_ptr_t.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <statdaemons/ext/range.hpp>
#include <mysqlxx/Pool.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/LayeredConfiguration.h>

namespace DB
{

class MysqlBlockInputStream final : public IProfilingBlockInputStream
{
public:
	MysqlBlockInputStream(mysqlxx::Query query, const Block & sample_block, const std::size_t max_block_size)
	: query{std::move(query)}, result{query.use()}, sample_block{sample_block}, max_block_size{max_block_size}
	{
	}

	String getName() const override { return "MysqlBlockInputStream"; }

	String getID() const override
	{
		return "Mysql(" + query.str() + ")";
	}

private:
	Block readImpl() override
	{
		auto block = sample_block.cloneEmpty();

		std::size_t rows = 0;
		while (auto row = result.fetch())
		{
			for (const auto idx : ext::range(0, row.size()))
				/// @todo type switch to get the real value from row[idx]
				block.getByPosition(idx).column->insert(Field{});

			++rows;
			if (rows == max_block_size)
				break;
		}

		return block;
	}

	mysqlxx::Query query;
	mysqlxx::UseQueryResult result;
	Block sample_block;
	std::size_t max_block_size;
};

class MysqlDictionarySource final : public IDictionarySource
{
	static const auto max_block_size = 8192;

public:
	MysqlDictionarySource(Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
		Block & sample_block, const Context & context)
		: layered_config_ptr{getLayeredConfig(config)},
		  pool{*layered_config_ptr, config_prefix},
		  sample_block{sample_block}, context(context) {}

private:
	BlockInputStreamPtr loadAll() override
	{
		auto connection = pool.Get();
		auto query = connection->query("SELECT 1+1;");
		auto result = query.use();
		while (auto row = result.fetch())
		{
			for (const auto idx : ext::range(0, row.size()))
				std::cout << row[idx].getString() << ' ';
			std::cout << std::endl;
		}
		return new MysqlBlockInputStream{pool.Get()->query(""), sample_block, max_block_size};
	}

	BlockInputStreamPtr loadId(const std::uint64_t id) override
	{
		throw Exception{
			"Method unsupported",
			ErrorCodes::NOT_IMPLEMENTED
		};
	}

	BlockInputStreamPtr loadIds(const std::vector<std::uint64_t> ids) override
	{
		throw Exception{
			"Method unsupported",
			ErrorCodes::NOT_IMPLEMENTED
		};
	}

	static config_ptr_t<Poco::Util::LayeredConfiguration> getLayeredConfig(Poco::Util::AbstractConfiguration & config)
	{
		config_ptr_t<Poco::Util::LayeredConfiguration> layered_config{new Poco::Util::LayeredConfiguration};
		layered_config->add(&config);
		return layered_config;
	}

	const config_ptr_t<Poco::Util::LayeredConfiguration> layered_config_ptr;
	mysqlxx::Pool pool;
	Block sample_block;
	const Context & context;
};

}
