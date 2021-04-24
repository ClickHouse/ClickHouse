#pragma once

#include <Columns/IColumn.h>
#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexGranuleBloomFilter.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class MergeTreeIndexConditionBloomFilter : public IMergeTreeIndexCondition
{
public:
    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_HAS,
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            FUNCTION_UNKNOWN, /// Can take any value.
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        RPNElement(Function function_ = FUNCTION_UNKNOWN) : function(function_) {}

        Function function = FUNCTION_UNKNOWN;
        std::vector<std::pair<size_t, ColumnPtr>> predicate;
    };

    MergeTreeIndexConditionBloomFilter(const SelectQueryInfo & info_, const Context & context_, const Block & header_, size_t hash_functions_);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule) const override
    {
        if (const auto & bf_granule = typeid_cast<const MergeTreeIndexGranuleBloomFilter *>(granule.get()))
            return mayBeTrueOnGranule(bf_granule);

        throw Exception("LOGICAL ERROR: require bloom filter index granule.", ErrorCodes::LOGICAL_ERROR);
    }

private:
    const Block & header;
    const Context & context;
    const SelectQueryInfo & query_info;
    const size_t hash_functions;
    std::vector<RPNElement> rpn;

    SetPtr getPreparedSet(const ASTPtr & node);

    bool mayBeTrueOnGranule(const MergeTreeIndexGranuleBloomFilter * granule) const;

    bool traverseAtomAST(const ASTPtr & node, Block & block_with_constants, RPNElement & out);

    bool traverseFunction(const ASTPtr & node, Block & block_with_constants, RPNElement & out, const ASTPtr & parent);

    bool traverseASTIn(const String & function_name, const ASTPtr & key_ast, const SetPtr & prepared_set, RPNElement & out);

    bool traverseASTIn(
        const String & function_name, const ASTPtr & key_ast, const DataTypePtr & type, const ColumnPtr & column, RPNElement & out);

    bool traverseASTEquals(
        const String & function_name, const ASTPtr & key_ast, const DataTypePtr & value_type, const Field & value_field, RPNElement & out, const ASTPtr & parent);
};

}
