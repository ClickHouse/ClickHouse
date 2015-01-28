#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/OwningBufferBlockInputStream.h>

namespace DB
{

class FileDictionarySource final : public IDictionarySource
{
	static const auto max_block_size = 8192;

public:
	FileDictionarySource(const std::string & filename, const std::string & format, Block & sample_block,
		const Context & context)
		: filename{filename}, format{format}, sample_block{sample_block}, context(context) {}

private:
	BlockInputStreamPtr loadAll() override
	{
		auto in_ptr = ext::make_unique<ReadBufferFromFile>(filename);
		auto stream = context.getFormatFactory().getInput(
			format, *in_ptr, sample_block, max_block_size, context.getDataTypeFactory());

		return new OwningBufferBlockInputStream{stream, std::move(in_ptr)};
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

	const std::string filename;
	const std::string format;
	Block sample_block;
	const Context & context;
};

}
