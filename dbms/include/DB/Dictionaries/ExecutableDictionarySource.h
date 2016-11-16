#pragma once

#include <DB/Dictionaries/IDictionarySource.h>

namespace DB
{


/// Allows loading dictionaries from executable
class ExecutableDictionarySource final : public IDictionarySource
{
	static constexpr auto max_block_size = 8192;

public:

	ExecutableDictionarySource(const std::string & name, const std::string & format, Block & sample_block,
		const Context & context);

	ExecutableDictionarySource(const ExecutableDictionarySource & other);

	BlockInputStreamPtr loadAll() override;

	BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

	BlockInputStreamPtr loadKeys(
		const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows) override;

	bool isModified() const override;

	bool supportsSelectiveLoad() const override;

	DictionarySourcePtr clone() const override;

	std::string toString() const override;

private:
	Logger * log = &Logger::get("ExecutableDictionarySource");

	LocalDateTime getLastModification() const;

	const std::string name;
	const std::string format;
	Block sample_block;
	const Context & context;
	LocalDateTime last_modification;
};

}
