#pragma once

#include <Common/SharedLibrary.h>
#include <Common/Bridge/LibraryBridgeHelper.h>
#include <common/LocalDateTime.h>
#include <Common/thread_local_rng.h>
#include "DictionaryStructure.h"
#include <Core/ExternalResultDescription.h>
#include "IDictionarySource.h"


namespace Poco
{
class Logger;

namespace Util
{
    class AbstractConfiguration;
}
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class CStringsHolder;
using LibraryBridgeHelperPtr = std::shared_ptr<LibraryBridgeHelper>;

class LibraryDictionarySource final : public IDictionarySource
{
public:
    LibraryDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix_,
        Block & sample_block_,
        const Context & context_,
        bool check_config);

    LibraryDictionarySource(const LibraryDictionarySource & other);
    LibraryDictionarySource & operator=(const LibraryDictionarySource &) = delete;

    ~LibraryDictionarySource() override;

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadUpdatedAll() override
    {
        throw Exception{"Method loadUpdatedAll is unsupported for LibraryDictionarySource", ErrorCodes::NOT_IMPLEMENTED};
    }

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(const Columns & key_columns, const std::vector<std::size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override;

    ///Not yet supported
    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

private:
    String getLibrarySettingsString(const Poco::Util::AbstractConfiguration & config, const std::string & config_root);

    String getDictAttributesString();

    String getDictIdsString(const std::vector<UInt64> & ids);

    LocalDateTime getLastModification() const;

    Poco::Logger * log;

    const DictionaryStructure dict_struct;

    const std::string config_prefix;

    const std::string path;

    const std::string dictionary_id;

    Block sample_block;

    Context context;

    LibraryBridgeHelperPtr bridge_helper;

    ExternalResultDescription description;

    String createDictID()
    {
        std::uniform_int_distribution<int> distribution('a', 'z');
        String random_str(16, ' ');
        for (auto & c : random_str)
            c = distribution(thread_local_rng);
        return random_str;
    }
};

}
