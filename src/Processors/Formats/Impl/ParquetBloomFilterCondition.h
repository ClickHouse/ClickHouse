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
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_HAS,
            FUNCTION_HAS_ANY,
            FUNCTION_HAS_ALL,
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

        RPNElement(Function function_ = FUNCTION_UNKNOWN) : function(function_) {} /// NOLINT

        Function function = FUNCTION_UNKNOWN;
        std::vector<std::pair<size_t, ColumnPtr>> predicate;
    };

    ParquetBloomFilterCondition(const ActionsDAGPtr & filter_actions_dag,
                                const IndexToColumnBF & index_to_column_hasher,
                                ContextPtr context_,
                                const Block & header_);

    bool mayBeTrueOnRowGroup(const IndexToColumnBF & bf);

private:
    const Block & header;
    std::vector<RPNElement> rpn;

    bool extractAtomFromTree(const RPNBuilderTreeNode & node,
                             const IndexToColumnBF & index_to_column_hasher,
                             RPNElement & out);

    bool traverseFunction(const RPNBuilderTreeNode & node,
                          const IndexToColumnBF & index_to_column_hasher,
                          RPNElement & out);

    bool traverseTreeIn(
        const String & function_name,
        const RPNBuilderTreeNode & key_node,
        const ConstSetPtr & prepared_set,
        const DataTypePtr & type,
        const ColumnPtr & column,
        const IndexToColumnBF & index_to_column_hasher,
        RPNElement & out);

    bool traverseTreeEquals(
        const String & function_name,
        const RPNBuilderTreeNode & key_node,
        const DataTypePtr & value_type,
        const Field & value_field,
        const IndexToColumnBF & index_to_column_hasher,
        RPNElement & out);
};

}

#endif
