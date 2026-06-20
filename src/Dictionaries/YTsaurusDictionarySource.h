#pragma once

#include "config.h"

#if USE_YTSAURUS
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionarySource.h>
#include <Storages/YTsaurus/StorageYTsaurus.h>
#include <Core/YTsaurus/YTsaurusClient.h>

#include <Common/VectorWithMemoryTracking.h>
#include <Core/Block.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Split `vec` into chunks of at most `chunk_size` elements; `chunk_size == 0` means a single unlimited chunk.
/// An empty input must yield no chunks at all: a YTsaurus selective load builds one lookup block per chunk, and an
/// empty key set must stay a no-op. Otherwise the empty-batch guard in `YTsaurusSourceFactory::createPipe`
/// (`lookup_input_blocks->empty()`) is bypassed, and an empty `lookup_rows` request is issued (consuming a throttler
/// token) even though there is nothing to fetch. The empty check must come before the `chunk_size == 0` branch, which
/// would otherwise return one empty chunk for an empty input.
template <typename T>
VectorWithMemoryTracking<VectorWithMemoryTracking<T>> divideVectorByChunkSize(const VectorWithMemoryTracking<T> & vec, size_t chunk_size)
{
    if (vec.empty())
        return {};
    if (!chunk_size)
        return {vec};

    VectorWithMemoryTracking<VectorWithMemoryTracking<T>> result;
    for (size_t i = 0; i < vec.size(); i += chunk_size)
    {
        auto start = vec.begin() + i;
        auto end = (i + chunk_size < vec.size()) ? start + chunk_size : vec.end();
        result.emplace_back(start, end);
    }
    return result;
}


class YTsarususDictionarySource final : public IDictionarySource
{
public:
    YTsarususDictionarySource(
        ContextPtr context_,
        const DictionaryStructure & dict_struct_,
        std::shared_ptr<YTsaurusStorageConfiguration> configuration_,
        const Block & sample_block_,
        const String & name);

    YTsarususDictionarySource(const YTsarususDictionarySource & other);

    ~YTsarususDictionarySource() override;

    BlockIO loadAll() override;

    BlockIO loadUpdatedAll() override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for YTsarususDictionarySource");
    }

    bool supportsSelectiveLoad() const override;

    BlockIO loadIds(const VectorWithMemoryTracking<UInt64> & ids) override;

    BlockIO loadKeys(const Columns & key_columns, const VectorWithMemoryTracking<size_t> & requested_rows) override;

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
    const ThrottlerPtr lookup_throttler;
    const String name;
};

}
#endif
