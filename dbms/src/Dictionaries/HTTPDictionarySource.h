#pragma once

#include <Poco/URI.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <common/LocalDateTime.h>
#include <IO/ConnectionTimeouts.h>

namespace Poco { class Logger; }


namespace DB
{

/// Allows loading dictionaries from http[s] source
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

    BlockInputStreamPtr loadUpdatedAll() override;

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(
        const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override;

    bool hasUpdateField() const override;

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

private:
    void getUpdateFieldAndDate(Poco::URI & uri);

    Poco::Logger * log;

    LocalDateTime getLastModification() const;

    std::chrono::time_point<std::chrono::system_clock> update_time;
    const DictionaryStructure dict_struct;
    const std::string url;
    std::string update_field;
    const std::string format;
    Block sample_block;
    const Context & context;
    ConnectionTimeouts timeouts;
};

}
