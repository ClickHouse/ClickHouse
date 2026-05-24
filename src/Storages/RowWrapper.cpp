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

std::optional<RowWrapperInfo> tryDescribeRowWrapper(const ColumnDescription & column)
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
    if (args.size() != field_names.size())
        return std::nullopt;

    Names wrapped;
    wrapped.reserve(args.size());
    for (size_t i = 0; i < args.size(); ++i)
    {
        const auto * id = args[i]->as<ASTIdentifier>();
        if (!id || id->name() != field_names[i])
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
        auto desc = tryDescribeRowWrapper(col);
        if (!desc)
            continue;

        for (const auto & wrapped : desc->wrapped_columns)
        {
            if (!columns.tryGet(wrapped))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Row wrapper column '{}' references unknown column '{}'", col.name, wrapped);

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
