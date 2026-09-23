#include <Interpreters/validateParameterizedViewSchema.h>

#include <Storages/ColumnsDescription.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <cstddef>
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

    auto throw_schema_mismatch = [&table_name](const String &details)
    {
        throw Exception(
            ErrorCodes::TYPE_MISMATCH,
            "After parameters substitution of parameterized view {} the actual schema does not match the defined one: {}",
            backQuoteIfNeed(table_name),
            details);
    };

    if (declared_columns.size() != actual_names_and_types.size())
        throw_schema_mismatch(fmt::format("expected {} columns, but got {}", declared_columns.size(), actual_names_and_types.size()));

    size_t i = 0;
    for (const auto [declared_column, actual_column] : std::views::zip(declared_columns.getAll(), actual_names_and_types))
    {
        if (declared_column != actual_column)
            throw_schema_mismatch(
                fmt::format("column {}: expected {}, but got {}", i, declared_column.dump(), actual_column.dump())
            );
        ++i;
    }
}

}
