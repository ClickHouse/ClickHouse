#pragma once

#include <Columns/IColumn.h>
#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexGranuleBloomFilter.h>

namespace DB
{

class MergeTreeIndexConditionBloomFilter : public IIndexCondition
{
public:
    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
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

    MergeTreeIndexConditionBloomFilter(const SelectQueryInfo & info, const Context & context, const Block & header, size_t hash_functions);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule) const override
    {
        if (const auto & bf_granule = typeid_cast<const MergeTreeIndexGranuleBloomFilter *>(granule.get()))
        {
            return mayBeTrueOnGranule(bf_granule);
        }

        throw Exception("LOGICAL ERROR: require bloom filter index granule.", ErrorCodes::LOGICAL_ERROR);
    }

private:
    const Block & header;
    const SelectQueryInfo & query_info;
    const size_t hash_functions;
    std::vector<RPNElement> rpn;

    bool mayBeTrueOnGranule(const MergeTreeIndexGranuleBloomFilter * granule) const;

    bool traverseAtomAST(const ASTPtr & node, Block & block_with_constants, RPNElement & out);

    bool processInOrNotInOperator(const String &function_name, const ASTPtr &key_ast, const ASTPtr &expr_list, RPNElement &out);

    bool processEqualsOrNotEquals(const String & function_name, const ASTPtr & key_ast, const DataTypePtr & value_type, const Field & value_field, RPNElement & out);
};

}
