#pragma once

#include <Common/SharedLibrary.h>
#include <Bridge/LibraryBridgeHelper.h>
#include <common/LocalDateTime.h>
#include <Core/UUID.h>
#include "DictionaryStructure.h"
#include <Core/ExternalResultDescription.h>
#include "IDictionarySource.h"
#include <Interpreters/Context_fwd.h>


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
        ContextPtr context_,
        bool created_from_ddl);

    LibraryDictionarySource(const LibraryDictionarySource & other);
    LibraryDictionarySource & operator=(const LibraryDictionarySource &) = delete;

    ~LibraryDictionarySource() override;

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadUpdatedAll() override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for LibraryDictionarySource");
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
    static String getDictIdsString(const std::vector<UInt64> & ids);

    String getDictAttributesString();

    static String getLibrarySettingsString(const Poco::Util::AbstractConfiguration & config, const std::string & config_root);

    static Field getDictID() { return UUIDHelpers::generateV4(); }

    Poco::Logger * log;

    const DictionaryStructure dict_struct;
    const std::string config_prefix;
    const std::string path;
    const Field dictionary_id;

    Block sample_block;
    ContextPtr context;

    LibraryBridgeHelperPtr bridge_helper;
    ExternalResultDescription description;
};

}
