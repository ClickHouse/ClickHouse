#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Storages/MergeTree/RPNBuilder.h>

#if USE_PARQUET

namespace parquet
{
    class BloomFilter;
}

namespace DB
{

class ParquetBloomFilterCondition
{
public:
    using IndexToColumnBF = std::unordered_map<std::size_t, std::unique_ptr<parquet::BloomFilter>>;
    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_EQUALS, // in range
            FUNCTION_NOT_EQUALS, // not in range
            FUNCTION_HAS, // tbd
            FUNCTION_HAS_ANY, // tbd
            FUNCTION_HAS_ALL, // tbd
            FUNCTION_IN, // in set
            FUNCTION_NOT_IN, // not in set
            FUNCTION_UNKNOWN, /// Can take any value.
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        RPNElement(Function function_ = FUNCTION_UNKNOWN) : function(function_) {} /// NOLINT

        Function function = FUNCTION_UNKNOWN;
        std::vector<std::pair<size_t, ColumnPtr>> predicate;
    };

    explicit ParquetBloomFilterCondition(const std::vector<RPNElement> & rpn_);

    bool mayBeTrueOnRowGroup(const IndexToColumnBF & bf);

private:
    std::vector<RPNElement> rpn;
};

struct BloomFilterRPNBuilder
{
    using IndexToColumnBF = ParquetBloomFilterCondition::IndexToColumnBF;
    using RPNElement = ParquetBloomFilterCondition::RPNElement;

    static std::vector<ParquetBloomFilterCondition::RPNElement> build(const ActionsDAGPtr & filter_actions_dag,
                                                                      const IndexToColumnBF & index_to_column_hasher,
                                                                      ContextPtr context_,
                                                                      const Block & header_);

private:
    static bool extractAtomFromTree(const RPNBuilderTreeNode & node,
                                    const IndexToColumnBF & index_to_column_hasher,
                                    const Block & header,
                                    RPNElement & out);

    static bool traverseFunction(const RPNBuilderTreeNode & node,
                                 const IndexToColumnBF & index_to_column_hasher,
                                 const Block & header,
                                 RPNElement & out);

    static bool traverseTreeIn(
        const String & function_name,
        const RPNBuilderTreeNode & key_node,
        const ConstSetPtr & prepared_set,
        const DataTypePtr & type,
        const ColumnPtr & column,
        const IndexToColumnBF & index_to_column_hasher,
        const Block & header,
        RPNElement & out);

    static bool traverseTreeEquals(
        const String & function_name,
        const RPNBuilderTreeNode & key_node,
        const DataTypePtr & value_type,
        const Field & value_field,
        const IndexToColumnBF & index_to_column_hasher,
        const Block & header,
        RPNElement & out);
};

}

#endif
