#include <Interpreters/validateParameterizedViewSchema.h>

#include <Storages/ColumnsDescription.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

void validateParameterizedViewSchema(
    const String & table_name,
    const NamesAndTypesList & actual_names_and_types,
    const ColumnsDescription & declared_columns)
{
    if (declared_columns.empty())
        return;

    auto throw_schema_mismatch = [&table_name]()
    {
        throw Exception(
            ErrorCodes::TYPE_MISMATCH,
            "After parameters substitution of parameterized view {} the actual schema does not match the defined one",
            backQuoteIfNeed(table_name));
    };

    if (declared_columns.size() != actual_names_and_types.size())
        throw_schema_mismatch();

    for (const auto [declared_column, actual_column] : std::views::zip(declared_columns.getAll(), actual_names_and_types))
    {
        if (declared_column.name != actual_column.name || declared_column.type->getName() != actual_column.type->getName())
            throw_schema_mismatch();
    }
}

}
