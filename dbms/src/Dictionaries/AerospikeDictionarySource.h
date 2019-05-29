#pragma once

#include <Common/config.h>
#include <Core/Block.h>
#if USE_AEROSPIKE

#    include "DictionaryStructure.h"
#    include "IDictionarySource.h"
#    include <aerospike/aerospike.h>

namespace DB
{
/// Allows loading dictionaries from Aerospike collection
class AerospikeDictionarySource final : public IDictionarySource
{
public:
    AerospikeDictionarySource(
        const DictionaryStructure & dict_struct,
        const std::string& host,
        UInt16 port,
        const std::string & namespace_name,
        const std::string & set_name,
        as_config * config,
        const Block & sample_block);
    AerospikeDictionarySource(
        const DictionaryStructure & dict_struct,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        Block & sample_block);

    AerospikeDictionarySource(const AerospikeDictionarySource & other);

    ~AerospikeDictionarySource() override;

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadUpdatedAll() override
    {
        throw Exception{"Method loadUpdatedAll is unsupported for AerospikeDictionarySource", ErrorCodes::NOT_IMPLEMENTED};
    }

    bool supportsSelectiveLoad() const override { return true; }

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(const Columns & /* key_columns */, const std::vector<size_t> & /* requested_rows */) override
    {
            //Aerospke does not support native indexing
            throw Exception{"Method loadKeys is unsupported for AerospikeDictionarySource", ErrorCodes::NOT_IMPLEMENTED};
    }

    bool isModified() const override { return true; }

    ///Not yet supported
    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override { return std::make_unique<AerospikeDictionarySource>(*this); }

    std::string toString() const override;
private:
    const DictionaryStructure dict_struct;
    const std::string host;
    const UInt16 port;
    const std::string namespace_name;
    const std::string set_name;
    Block sample_block;
    aerospike client; // may be use ptr here
    // may be save global error variable
};

}

#endif

