#pragma once

#include "config.h"

#if USE_YTSAURUS
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionarySource.h>
#include <Storages/YTsaurus/StorageYTsaurus.h>
#include <Core/YTsaurus/YTsaurusClient.h>

#include <Core/Block.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


class YTsarususDictionarySource final : public IDictionarySource
{
public:
    YTsarususDictionarySource(
        ContextPtr context_,
        const DictionaryStructure & dict_struct_,
        std::shared_ptr<YTsaurusStorageConfiguration> configuration_,
        const Block & sample_block_);

    YTsarususDictionarySource(const YTsarususDictionarySource & other);

    ~YTsarususDictionarySource() override;

    QueryPipeline loadAll() override;

    QueryPipeline loadUpdatedAll() override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for YTsarususDictionarySource");
    }

    bool supportsSelectiveLoad() const override;

    QueryPipeline loadIds(const std::vector<UInt64> & ids) override;

    QueryPipeline loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override { return true; }

    /// Not yet supported
    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override { return std::make_shared<YTsarususDictionarySource>(*this); }

    std::string toString() const override;

private:
    ContextPtr context;
    const DictionaryStructure dict_struct;
    const std::shared_ptr<YTsaurusStorageConfiguration> configuration;
    SharedHeader sample_block;
    YTsaurusClientPtr client;
};

}
#endif
