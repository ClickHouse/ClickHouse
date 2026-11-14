#pragma once

#include <Processors/TTL/ITTLAlgorithm.h>

namespace DB
{

/// Deletes (replaces to default) values in column according to column's TTL description.
/// If all values in column are replaced with defaults, this column won't be written to part,
/// unless it is a `Nested` subcolumn that shares the array sizes with the other subcolumns.
class TTLColumnAlgorithm final : public ITTLAlgorithm
{
public:
    TTLColumnAlgorithm(
        const TTLExpressions & ttl_expressions_,
        const TTLDescription & description_,
        const TTLInfo & old_ttl_info_,
        time_t current_time_,
        bool force_,
        const String & column_name_,
        const ExpressionActionsPtr & default_expression_,
        const String & default_column_name_,
        bool is_compact_part_,
        bool shares_nested_offsets_
    );

    void execute(Block & block) override;
    void finalize(const MutableDataPartPtr & data_part) const override;

    /// The default of a `Nested` subcolumn keeps the array sizes, which it shares with the other subcolumns.
    static ColumnPtr createArraysOfDefaults(const IDataType & type, const IColumn & column_with_sizes);

private:
    const String column_name;
    const ExpressionActionsPtr default_expression;
    const String default_column_name;

    bool is_fully_empty = true;
    bool is_compact_part;
    bool shares_nested_offsets;
};

}
