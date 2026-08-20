#pragma once

#include <Core/Block.h>
#include <base/BorrowedObjectPool.h>

#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionarySource.h>
#include <Storages/RedisCommon.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int NOT_IMPLEMENTED;
    }

    class RedisDictionarySource final : public IDictionarySource
    {
    public:
        RedisDictionarySource(
            const DictionaryStructure & dict_struct_,
            const RedisConfiguration & configuration_,
            SharedHeader sample_block_);

        RedisDictionarySource(const RedisDictionarySource & other);

        ~RedisDictionarySource() override;

        BlockIO loadAll() override;

        BlockIO loadUpdatedAll() override
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for RedisDictionarySource");
        }

        bool supportsSelectiveLoad() const override { return true; }

        BlockIO loadIds(const VectorWithMemoryTracking<UInt64> & ids) override;

        BlockIO loadKeys(const Columns & key_columns, const VectorWithMemoryTracking<size_t> & requested_rows) override;

        bool isModified() const override { return true; }

        bool hasUpdateField() const override { return false; }

        DictionarySourcePtr clone() const override { return std::make_shared<RedisDictionarySource>(*this); }

        std::string toString() const override;

    private:
        const DictionaryStructure dict_struct;
        const RedisConfiguration configuration;

        RedisPoolPtr pool;
        SharedHeader sample_block;
    };
}
