#pragma once

#include <DB/Dictionaries/IDictionarySource.h>

namespace DB
{


/// Allows loading dictionaries from executable
class HTTPDictionarySource final : public IDictionarySource
{
	static constexpr auto max_block_size = 8192;

public:

	HTTPDictionarySource(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, Block & sample_block,
		const Context & context);

	HTTPDictionarySource(const HTTPDictionarySource & other);


	BlockInputStreamPtr loadAll() override;

	BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

	BlockInputStreamPtr loadKeys(
		const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows) override;

	bool isModified() const override;

	bool supportsSelectiveLoad() const override;

	DictionarySourcePtr clone() const override;

	//DictionarySourcePtr clone() const override { return std::make_unique<HTTPDictionarySource>(*this); }

	std::string toString() const override;

private:
	Logger * log = &Logger::get("HTTPDictionarySource");

	LocalDateTime getLastModification() const;

	const std::string host;
	int port;
	const std::string path;

	const std::string format;
	Block sample_block;
	const Context & context;
	const std::string load_all_query;
	LocalDateTime last_modification;
};

}
