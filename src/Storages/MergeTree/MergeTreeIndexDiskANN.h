#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <memory>
#include <random>
#include <string_view>

#include "IO/WriteBuffer.h"
#include "index.h"

namespace DB
{

using DiskANNIndex = diskann::Index<Float32>;
using DiskANNIndexPtr = std::shared_ptr<DiskANNIndex>;

// !TODO: Working only with Float32 type
using DiskANNValue = Float32;

struct MergeTreeIndexGranuleDiskANN final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleDiskANN(const String & index_name_, const Block & index_sample_block_);
    MergeTreeIndexGranuleDiskANN(
        const String & index_name_, const Block & index_sample_block_, 
        DiskANNIndexPtr base_index_, uint32_t dimensions, std::vector<DiskANNValue> datapoints
    );

    ~MergeTreeIndexGranuleDiskANN() override = default;

    void serializeBinary(WriteBuffer & out) const override;
    uint64_t calculateIndexSize() const;

    void deserializeBinary(ReadBuffer & in, MergeTreeIndexVersion version) override;
    bool empty() const override { return false; }

    std::optional<uint32_t> dimensions;
    std::vector<DiskANNValue> datapoints;

    String index_name;
    Block index_sample_block;
    DiskANNIndexPtr base_index;
};


struct MergeTreeIndexAggregatorDiskANN final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorDiskANN(const String & index_name_, const Block & index_sample_block);
    ~MergeTreeIndexAggregatorDiskANN() override = default;

    bool empty() const override { return accumulated_data.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    void flattenAccumulatedData(std::vector<std::vector<DiskANNValue>> data);

private:
    String index_name;
    Block index_sample_block;

    std::optional<uint32_t> dimensions;
    std::vector<DiskANNValue> accumulated_data;
};


class MergeTreeIndexConditionDiskANN final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionDiskANN(
        const IndexDescription & index,
        const SelectQueryInfo & query,
        ContextPtr context
    );

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionDiskANN() override = default;
private:
    // Type of the vector to use as a target in the distance function
    using Target = std::vector<float>;

    // Extracted data from the query like WHERE L2Distance(column_name, target) < distance
    struct ANNExpression {
        Target target;
        float distance;
    };

    using ANNExpressionOpt = std::optional<ANNExpression>;

    // Item of the Reverse Polish notation
    struct RPNElement
    {
        enum Function
        {
            // Atoms of an ANN expression

            // Function like L2Distance
            FUNCTION_DISTANCE, 

            // Function like tuple(...)
            FUNCTION_TUPLE,

            // Operator <
            FUNCTION_LESS,

            // Numeric float value
            FUNCTION_FLOAT_LITERAL,

            // Identifier of the column, e.g. L2Distance(number, target), number is a identifier of the column
            FUNCTION_IDENTIFIER,

            FUNCTION_UNKNOWN, /// Can take any value.
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
        };

        explicit RPNElement(Function function_ = FUNCTION_UNKNOWN)
        : function(function_)
        {}

        Function function;

        // TODO: Use not optional, but variant
        // Value for the FUNCTION_FLOAT_LITERAL
        std::optional<float> literal;

        // Value for the FUNCTION_IDENTIDIER
        std::optional<String> identifier;
    };

    using RPN = std::vector<RPNElement>;

    // Build RPN of the query, return with copy ellision
    RPN buildRPN(const SelectQueryInfo & query, ContextPtr context);

    // Util functions for the traversal of AST
    void traverseAST(const ASTPtr & node, RPN & rpn);
    // Return true if we can identify our node type
    bool traverseAtomAST(const ASTPtr & node, RPNElement & out);

    // Check that rpn matches the template rpn (TODO: put template RPN outside this function)
    bool matchRPN(const RPN & rpn);

    Block block_with_constants;

    DataTypes index_data_types;
    ANNExpressionOpt expression;
};


class MergeTreeIndexDiskANN : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexDiskANN(const IndexDescription & index_)
        : IMergeTreeIndex(index_)
    {}

    ~MergeTreeIndexDiskANN() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const override { return true; }

    const char* getSerializedFileExtension() const override { return ".idx2"; }
    MergeTreeIndexFormat getDeserializedFormat(const DiskPtr disk, const std::string & path_prefix) const override;
};

}
