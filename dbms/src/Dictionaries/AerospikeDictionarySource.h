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

    /// @TODO(glebx777): fix it
    BlockInputStreamPtr loadIds(const std::vector<UInt64> & /*ids*/) override;
    /* override {
            throw Exception{"Method loadIds is unsupported for AerospikeDictionarySource", ErrorCodes::NOT_IMPLEMENTED};
    } */

    BlockInputStreamPtr loadKeys(const Columns & /* key_columns */, const std::vector<size_t> & /* requested_rows */) override
    {
            //Aerospke does not support native indexing
            throw Exception{"Method loadKeys is unsupported for AerospikeDictionarySource", ErrorCodes::NOT_IMPLEMENTED};
    }

    /// @todo: for Aerospike, modification date can somehow be determined from the `_id` object field
    bool isModified() const override { return true; }

    ///Not yet supported
    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override { return std::make_unique<AerospikeDictionarySource>(*this); }

    std::string toString() const override;
private:
    const DictionaryStructure dict_struct;
    std::string host; // think how to save const here
    UInt16 port; // think how to save const here
    Block sample_block;
    aerospike client; // may be use ptr here
    // may be save global error variable
};

}

#endif

