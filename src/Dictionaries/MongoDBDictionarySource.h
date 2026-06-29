#pragma once

#include "config.h"

#if USE_MONGODB
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionarySource.h>
#include <QueryPipeline/BlockIO.h>

#include <Core/Block.h>
#include <Storages/StorageMongoDB.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Allows loading dictionaries from a MongoDB collection
class MongoDBDictionarySource final : public IDictionarySource
{
public:
    MongoDBDictionarySource(
        const DictionaryStructure & dict_struct_,
        std::shared_ptr<MongoDBConfiguration> configuration_,
        SharedHeader sample_block_);

    MongoDBDictionarySource(const MongoDBDictionarySource & other);

    ~MongoDBDictionarySource() override;

    BlockIO loadAll() override;

    BlockIO loadUpdatedAll() override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for MongoDBDictionarySource");
    }

    bool supportsSelectiveLoad() const override { return true; }

    BlockIO loadIds(const VectorWithMemoryTracking<UInt64> & ids) override;

    BlockIO loadKeys(const Columns & key_columns, const VectorWithMemoryTracking<size_t> & requested_rows) override;

    /// @todo: for MongoDB, modification date can somehow be determined from the `_id` object field
    bool isModified() const override { return true; }

    /// Not yet supported
    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override { return std::make_shared<MongoDBDictionarySource>(*this); }

    std::string toString() const override;

private:
    MongoDBInstanceHolder & instance_holder = MongoDBInstanceHolder::instance();

    const DictionaryStructure dict_struct;
    const std::shared_ptr<MongoDBConfiguration> configuration;
    SharedHeader sample_block;
};

}
#endif
