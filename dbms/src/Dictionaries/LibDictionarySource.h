#pragma once

#include <Common/SharedLibrary.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ExternalResultDescription.h>
#include <Dictionaries/IDictionarySource.h>

#include <iostream>

namespace Poco
{
class Logger;
}


namespace DB
{
struct CStringHolder;

/// Allows loading dictionaries from .so
class LibDictionarySource final : public IDictionarySource
{
public:
    LibDictionarySource(const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        Block & sample_block,
        const Context & context);

    LibDictionarySource(const LibDictionarySource & other);

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(const Columns & key_columns, const std::vector<std::size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override;

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

private:
    Poco::Logger * log;

    LocalDateTime getLastModification() const;

    const DictionaryStructure dict_struct;
    const std::string config_prefix;
    const std::string filename;
    Block sample_block;
    const Context & context;
    SharedLibraryPtr library;
    ExternalResultDescription description;
    std::shared_ptr<CStringHolder> settings;
};
}
