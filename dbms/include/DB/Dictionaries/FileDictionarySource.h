#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/OwningBufferBlockInputStream.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/DataStreams/FormatFactory.h>
#include <Poco/Timestamp.h>
#include <Poco/File.h>

namespace DB
{

/// Allows loading dictionaries from a file with given format, does not support "random access"
class FileDictionarySource final : public IDictionarySource
{
	static const auto max_block_size = 8192;

public:
	FileDictionarySource(const std::string & filename, const std::string & format, Block & sample_block,
		const Context & context)
		: filename{filename}, format{format}, sample_block{sample_block}, context(context)
	{}

	FileDictionarySource(const FileDictionarySource & other)
		: filename{other.filename}, format{other.format},
		  sample_block{other.sample_block}, context(other.context),
		  last_modification{other.last_modification}
	{}

	BlockInputStreamPtr loadAll() override
	{
		auto in_ptr = std::make_unique<ReadBufferFromFile>(filename);
		auto stream = context.getFormatFactory().getInput(
			format, *in_ptr, sample_block, max_block_size);
		last_modification = getLastModification();

		return new OwningBufferBlockInputStream{stream, std::move(in_ptr)};
	}

	BlockInputStreamPtr loadIds(const std::vector<std::uint64_t> & ids) override
	{
		throw Exception{
			"Method unsupported",
			ErrorCodes::NOT_IMPLEMENTED
		};
	}

	bool isModified() const override { return getLastModification() > last_modification; }
	bool supportsSelectiveLoad() const override { return false; }

	DictionarySourcePtr clone() const override { return std::make_unique<FileDictionarySource>(*this); }

	std::string toString() const override { return "File: " + filename + ' ' + format; }

private:
	Poco::Timestamp getLastModification() const { return Poco::File{filename}.getLastModified(); }

	const std::string filename;
	const std::string format;
	Block sample_block;
	const Context & context;
	Poco::Timestamp last_modification;
};

}
