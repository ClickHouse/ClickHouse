#pragma once

#include <Common/SharedLibrary.h>
#include <common/LocalDateTime.h>
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

/// Allows loading dictionaries from dynamic libraries (.so)
/// Experimental version
/// Example: tests/external_dictionaries/dictionary_library/dictionary_library.cpp
class LibraryDictionarySource final : public IDictionarySource
{
public:
    LibraryDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix_,
        Block & sample_block_,
        const Context & context,
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
    Poco::Logger * log;

    LocalDateTime getLastModification() const;

    const DictionaryStructure dict_struct;
    const std::string config_prefix;
    const std::string path;
    Block sample_block;
    SharedLibraryPtr library;
    ExternalResultDescription description;
    std::shared_ptr<CStringsHolder> settings;
    void * lib_data = nullptr;
};

}
