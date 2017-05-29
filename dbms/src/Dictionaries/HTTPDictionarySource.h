#pragma once

#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>


namespace Poco { class Logger; }


namespace DB
{

/// Allows loading dictionaries from executable
class HTTPDictionarySource final : public IDictionarySource
{
public:
    HTTPDictionarySource(const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        Block & sample_block,
        const Context & context);

    HTTPDictionarySource(const HTTPDictionarySource & other);

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(
        const Columns & key_columns, const std::vector<std::size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override;

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

private:
    Poco::Logger * log;

    LocalDateTime getLastModification() const;

    const DictionaryStructure dict_struct;
    const std::string url;
    const std::string format;
    Block sample_block;
    const Context & context;
};

}
