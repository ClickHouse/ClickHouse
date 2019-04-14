#pragma once

#include <Common/config.h>
#if USE_POCO_REDIS

#    include "DictionaryStructure.h"
#    include "IDictionarySource.h"

namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }

    namespace Redis
    {
        class Client;
    }
}


namespace DB
{
    class RedisDictionarySource final : public IDictionarySource
    {
        RedisDictionarySource(
                const DictionaryStructure & dict_struct,
                const std::string & host,
                UInt16 port,
                UInt8 db_index,
                const Block & sample_block);

    public:
        RedisDictionarySource(
                const DictionaryStructure & dict_struct,
                const Poco::Util::AbstractConfiguration & config,
                const std::string & config_prefix,
                Block & sample_block);

        RedisDictionarySource(const RedisDictionarySource & other);

        ~RedisDictionarySource() override;

        BlockInputStreamPtr loadAll() override;

        BlockInputStreamPtr loadUpdatedAll() override
        {
            throw Exception{"Method loadUpdatedAll is unsupported for RedisDictionarySource", ErrorCodes::NOT_IMPLEMENTED};
        }

        bool supportsSelectiveLoad() const override { return true; }

        BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

        BlockInputStreamPtr loadKeys(const Columns & /* key_columns */, const std::vector<size_t> & /* requested_rows */) override
        {
            throw Exception{"Method loadKeys is unsupported for RedisDictionarySource", ErrorCodes::NOT_IMPLEMENTED};
        }

        bool isModified() const override { return true; }

        bool hasUpdateField() const override { return false; }

        DictionarySourcePtr clone() const override { return std::make_unique<RedisDictionarySource>(*this); }

        std::string toString() const override;

    private:
        const DictionaryStructure dict_struct;
        const std::string host;
        const UInt16 port;
        const UInt8 db_index; // [0..15]
        Block sample_block;

        std::shared_ptr<Poco::Redis::Client> client;
    };

}
#endif
