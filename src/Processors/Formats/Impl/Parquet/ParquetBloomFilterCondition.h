#pragma once

#include <config.h>

#if USE_PARQUET

#include <Storages/MergeTree/KeyCondition.h>

namespace parquet
{
class BloomFilter;
}

namespace DB
{

class ParquetBloomFilterCondition
{
public:

    struct ConditionElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            /// Can take any value.
            FUNCTION_UNKNOWN,
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        using ColumnPtr = IColumn::Ptr;
        using Columns = std::vector<ColumnPtr>;
        using KeyColumns = std::vector<std::size_t>;

        Function function;
        Columns columns;
        KeyColumns key_columns;
    };

    using RPNElement = KeyCondition::RPNElement;
    using IndexColumnToColumnBF = std::unordered_map<std::size_t, std::unique_ptr<parquet::BloomFilter>>;

    explicit ParquetBloomFilterCondition(const std::vector<ConditionElement> & condition_);

    bool mayBeTrueOnRowGroup(const IndexColumnToColumnBF & column_index_to_column_bf) const;

private:
    std::vector<ParquetBloomFilterCondition::ConditionElement> condition;
};

std::vector<ParquetBloomFilterCondition::ConditionElement> keyConditionRPNToParquetBloomFilterCondition(
    const std::vector<KeyCondition::RPNElement> & rpn,
    const std::vector<DataTypePtr> & data_types,
    const ParquetBloomFilterCondition::IndexColumnToColumnBF & column_index_to_column_bf);

}

#endif
