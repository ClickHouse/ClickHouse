#pragma once

#include <IO/ConnectionTimeouts.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/URI.h>
#include <common/LocalDateTime.h>
#include "DictionaryStructure.h"
#include "IDictionarySource.h"
#include <Interpreters/Context.h>
#include <IO/CompressionMethod.h>

namespace Poco
{
class Logger;
}


namespace DB
{
/// Allows loading dictionaries from http[s] source
class HTTPDictionarySource final : public IDictionarySource
{
public:

    struct Configuration
    {
        const std::string url;
        const std::string format;
        const std::string update_field;
        const UInt64 update_lag;
        const ReadWriteBufferFromHTTP::HTTPHeaderEntries header_entries;
    };

    HTTPDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Configuration & configuration,
        const Poco::Net::HTTPBasicCredentials & credentials_,
        Block & sample_block_,
        ContextPtr context_,
        bool created_from_ddl);

    HTTPDictionarySource(const HTTPDictionarySource & other);
    HTTPDictionarySource & operator=(const HTTPDictionarySource &) = delete;

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadUpdatedAll() override;

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override;

    bool hasUpdateField() const override;

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

private:
    void getUpdateFieldAndDate(Poco::URI & uri);

    // wrap buffer using encoding from made request
    BlockInputStreamPtr createWrappedBuffer(std::unique_ptr<ReadWriteBufferFromHTTP> http_buffer);

    Poco::Logger * log;

    LocalDateTime getLastModification() const;

    std::chrono::time_point<std::chrono::system_clock> update_time;
    const DictionaryStructure dict_struct;
    const Configuration configuration;
    Poco::Net::HTTPBasicCredentials credentials;
    Block sample_block;
    ContextPtr context;
    ConnectionTimeouts timeouts;
};

}

