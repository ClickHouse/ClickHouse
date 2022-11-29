#pragma once

#include <BridgeHelper/ExternalDictionaryLibraryBridgeHelper.h>
#include <Common/LocalDateTime.h>
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
using ExternalDictionaryLibraryBridgeHelperPtr = std::shared_ptr<ExternalDictionaryLibraryBridgeHelper>;

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

    QueryPipeline loadAll() override;

    QueryPipeline loadUpdatedAll() override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for LibraryDictionarySource");
    }

    QueryPipeline loadIds(const std::vector<UInt64> & ids) override;

    QueryPipeline loadKeys(const Columns & key_columns, const std::vector<std::size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override;

    ///Not yet supported
    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

private:
    String getDictAttributesString();

    static String getLibrarySettingsString(const Poco::Util::AbstractConfiguration & config, const std::string & config_root);

    static Field getDictID() { return UUIDHelpers::generateV4(); }

    Poco::Logger * log;

    const DictionaryStructure dict_struct;
    const std::string config_prefix;
    std::string path;
    const Field dictionary_id;

    Block sample_block;
    ContextPtr context;

    ExternalDictionaryLibraryBridgeHelperPtr bridge_helper;
    ExternalResultDescription description;
};

}
