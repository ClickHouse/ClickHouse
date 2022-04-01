#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>

#include <faiss/IndexIVFFlat.h>
#include "Interpreters/Context_fwd.h"
#include "Parsers/IAST_fwd.h"
#include "Storages/SelectQueryInfo.h"
#include "base/types.h"

namespace DB
{

struct MergeTreeIndexGranuleIVFFlat final : public IMergeTreeIndexGranule
{
    using FaissBaseIndex = faiss::Index;
    using FaissBaseIndexPtr = std::shared_ptr<FaissBaseIndex>; 

    MergeTreeIndexGranuleIVFFlat(const String & index_name_, const Block & index_sample_block_);
    MergeTreeIndexGranuleIVFFlat(
        const String & index_name_, 
        const Block & index_sample_block_,
        FaissBaseIndexPtr index_base_);

    ~MergeTreeIndexGranuleIVFFlat() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;

    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override;

    String index_name;
    Block index_sample_block;
    FaissBaseIndexPtr index_base;
};


struct MergeTreeIndexAggregatorIVFFlat final : IMergeTreeIndexAggregator
{
    using Value = Float32;

    MergeTreeIndexAggregatorIVFFlat(const String & index_name_, const Block & index_sample_block_);
    ~MergeTreeIndexAggregatorIVFFlat() override = default;

    bool empty() const override;
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_name;
    Block index_sample_block;
    std::vector<Value> values;
};


class MergeTreeIndexConditionIVFFlat final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionIVFFlat(
        const IndexDescription & index,
        const SelectQueryInfo & query,
        ContextPtr context);
    ~MergeTreeIndexConditionIVFFlat() override = default;

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

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


class MergeTreeIndexIVFFlat : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexIVFFlat(const IndexDescription & index_);

    ~MergeTreeIndexIVFFlat() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;

    const char* getSerializedFileExtension() const override { return ".idx2"; }
    MergeTreeIndexFormat getDeserializedFormat(const DiskPtr disk, const std::string & path_prefix) const override;
};

}
