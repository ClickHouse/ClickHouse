#pragma once

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>

#include <Core/Range.h>
#include <Processors/Chunk.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Functions/IFunction.h>

namespace DB
{

#if USE_AVRO

class ChunkPartitioner
{
public:
    explicit ChunkPartitioner(
        Poco::JSON::Array::Ptr partition_specification,
        Poco::JSON::Array::Ptr schema_fields,
        ContextPtr context,
        SharedHeader sample_block_);

    using PartitionKey = Row;
    struct PartitionKeyHasher
    {
        size_t operator()(const PartitionKey & key) const;

        mutable std::hash<String> hasher;
    };

    std::vector<std::pair<PartitionKey, Chunk>> partitionChunk(const Chunk & chunk);

    const std::vector<String> & getColumns() const { return columns_to_apply; }

    const std::vector<DataTypePtr> & getResultTypes() const { return result_data_types; }

private:
    SharedHeader sample_block;

    std::vector<FunctionOverloadResolverPtr> functions;
    std::vector<std::optional<size_t>> function_params;
    std::vector<String> columns_to_apply;
    std::vector<DataTypePtr> result_data_types;

    size_t max_partitions_count;
};

class IcebergPartitionCalculator final : public ISimpleTransform
{
public:
    explicit IcebergPartitionCalculator(
        Poco::JSON::Array::Ptr partition_specification,
        Poco::JSON::Array::Ptr schema,
        ContextPtr context,
        SharedHeader sample_block_)
        : ISimpleTransform(sample_block_, sample_block_, true)
        , partitioner(partition_specification, schema, context, sample_block_)
        {}

    String getName() const override { return "IcebergStatisticsTransform"; }

    void transform(Chunk & chunk) override;
    Row getPartitionValue() const
    {
        return partition_value;
    }

    const ChunkPartitioner & getChunkPartitioner() const
    {
        return partitioner;
    }

protected:
    ChunkPartitioner partitioner;
    Row partition_value;
};

using IcebergPartitionCalculatorPtr = std::shared_ptr<IcebergPartitionCalculator>;


#endif

}
