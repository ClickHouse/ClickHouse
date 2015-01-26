#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Dictionaries/IDictionarySource.h>

namespace DB
{

const auto max_block_size = 8192;

class FileDictionarySource final : public IDictionarySource
{
public:
	FileDictionarySource(const std::string & filename, const std::string & format, Block & sample_block,
		const Context & context)
		: filename{filename}, format{format}, sample_block{sample_block}, context(context) {}

private:
	BlockInputStreamPtr loadAll() override
	{
		in_ptr = ext::make_unique<ReadBufferFromFile>(filename);
		return context.getFormatFactory().getInput(
			format, *in_ptr, sample_block, max_block_size, context.getDataTypeFactory());
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

	void reset() override
	{
		in_ptr.reset(nullptr);
	}

	const std::string filename;
	const std::string format;
	Block sample_block;
	const Context & context;

	std::unique_ptr<ReadBufferFromFile> in_ptr;
};

}
