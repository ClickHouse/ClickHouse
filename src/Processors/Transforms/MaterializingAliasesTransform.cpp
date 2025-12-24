#include <Processors/Transforms/MaterializingAliasesTransform.h>
#include <Processors/Formats/IInputFormat.h>
#include <Parsers/ASTIdentifier.h>
#include <Columns/IColumn.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ColumnDefault.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_COLUMN;
}

MaterializingAliasesTransform::MaterializingAliasesTransform(SharedHeader header, const ColumnDefaults & column_defaults, IInputFormat & input_format_)
    : ISimpleTransform(header, header, true)
    , aliases(MaterializingAliasesTransform::getAliasToColumnMap(column_defaults))
    , input_format(input_format_)
{
}

void MaterializingAliasesTransform::transform(Chunk & chunk)
{
    if (aliases.empty())
        return;

    const auto * block_missing_values = input_format.getMissingValues();
    if (!block_missing_values)
        return;

    const auto & header = getOutputPort().getHeader();
    size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    const auto column_has_values = [&](auto idx)
    {
        if (!block_missing_values->hasDefaultBits(idx))
            return true;
        return block_missing_values->getDefaultsBitmask(idx).count() != columns[idx]->size();
    };

    for (const auto & [alias, column] : aliases)
    {
        /// Both columns should be presented in the block, we only replace the values, not add new columns
        if (!header.has(alias) || !header.has(column))
            continue;

        size_t alias_idx = header.getPositionByName(alias);
        size_t column_idx = header.getPositionByName(column);

        /// Skip if alias column has no values
        if (!column_has_values(alias_idx))
            continue;

        /// Both columns were set in the input data, this is not supported (though it can be, but does not worth it)
        if (column_has_values(column_idx))
            throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Column '{}' is an ALIAS for '{}'. Cannot provide values for both columns", alias, column);

        /// At this point we are sure that alias column has values, and column itself does not.
        columns[column_idx] = columns[alias_idx];
    }

    chunk.setColumns(std::move(columns), num_rows);
}

NameToNameMap MaterializingAliasesTransform::getAliasToColumnMap(const ColumnDefaults & column_defaults)
{
    NameToNameMap res;
    for (const auto & [name, default_desc] : column_defaults)
    {
        if (default_desc.kind == ColumnDefaultKind::Alias)
        {
            const ASTPtr & alias_expression = default_desc.expression;
            if (const ASTIdentifier * actual_column = typeid_cast<const ASTIdentifier *>(alias_expression.get()))
                res[name] = actual_column->full_name;
        }
    }
    return res;
}

ColumnsWithTypeAndName MaterializingAliasesTransform::getColumnAliases(const ColumnsDescription & columns_description)
{
    ColumnsWithTypeAndName res;

    if (!columns_description.hasDefaults())
        return res;

    for (const auto & [name, default_desc] : columns_description.getDefaults())
    {
        if (default_desc.kind == ColumnDefaultKind::Alias)
        {
            const ASTPtr & alias_expression = default_desc.expression;
            if (typeid_cast<const ASTIdentifier *>(alias_expression.get()))
            {
                auto column = columns_description.getColumn(GetColumnsOptions::All, name);
                res.emplace_back(column.type, name);
            }
        }
    }

    return res;
}

}
