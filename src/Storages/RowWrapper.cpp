#include <Storages/RowWrapper.h>
#include <Storages/ColumnDefault.h>
#include <DataTypes/DataTypeRow.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <unordered_map>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::optional<RowWrapperInfo> tryDescribeRowWrapper(const ColumnDescription & column, const ColumnsDescription & all_columns)
{
    if (column.default_desc.kind != ColumnDefaultKind::Materialized)
        return std::nullopt;

    const auto * row_type = typeid_cast<const DataTypeRow *>(column.type.get());
    if (!row_type)
        return std::nullopt;

    const auto * fn = column.default_desc.expression ? column.default_desc.expression->as<ASTFunction>() : nullptr;
    if (!fn || fn->name != "tuple" || !fn->arguments)
        return std::nullopt;

    const auto & args = fn->arguments->children;
    const auto & field_names = row_type->getElementNames();
    const auto & field_types = row_type->getElements();
    if (args.size() != field_names.size())
        return std::nullopt;

    Names wrapped;
    wrapped.reserve(args.size());
    for (size_t i = 0; i < args.size(); ++i)
    {
        const auto * id = args[i]->as<ASTIdentifier>();
        if (!id || id->name() != field_names[i])
            return std::nullopt;

        /// A wrapper must mirror its source columns exactly: the optimizer replaces a read of
        /// the source column with `__rowElement(wrapper, i)` under the original name, so anything
        /// but type identity (including nullability and decimal scale) would silently change the
        /// type of that column. A `Row` whose field type merely differs is a valid ordinary
        /// materialized column, it is just not a wrapper.
        const auto * source = all_columns.tryGet(id->name());
        if (!source || !source->type->equals(*field_types[i]))
            return std::nullopt;

        /// A `MATERIALIZED` column reading an `EPHEMERAL` one is deliberately never recomputed by a
        /// mutation (see `MutationsInterpreter`), so an `ALTER ... UPDATE` of another source column
        /// would leave the stored wrapper holding the pre-mutation values while the source columns
        /// hold the new ones. Such a `Row` cannot stand in for its sources.
        if (source->default_desc.kind == ColumnDefaultKind::Ephemeral)
            return std::nullopt;

        wrapped.push_back(id->name());
    }

    return RowWrapperInfo{column.name, std::move(wrapped)};
}

std::vector<RowWrapperInfo> collectRowWrappers(const ColumnsDescription & columns)
{
    std::vector<RowWrapperInfo> wrappers;
    std::unordered_map<String, String> owned_by;

    for (const auto & col : columns)
    {
        auto desc = tryDescribeRowWrapper(col, columns);
        if (!desc)
            continue;

        for (const auto & wrapped : desc->wrapped_columns)
        {
            /// `tryDescribeRowWrapper` already guaranteed the source column exists.
            auto [it, inserted] = owned_by.try_emplace(wrapped, col.name);
            if (!inserted)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Column '{}' cannot be wrapped by both Row columns '{}' and '{}'",
                    wrapped, it->second, col.name);
        }

        wrappers.push_back(std::move(*desc));
    }

    return wrappers;
}

}
