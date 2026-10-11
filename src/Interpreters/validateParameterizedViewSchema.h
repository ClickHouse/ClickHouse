#pragma once

#include <Core/NamesAndTypes.h>

namespace DB
{

class ColumnsDescription;

/// Validates that the actual schema of a parameterized view (after parameter substitution)
/// matches its declared schema. Throws TYPE_MISMATCH if they differ.
void validateParameterizedViewSchema(
    const String & table_name,
    const NamesAndTypesList & actual_names_and_types,
    const ColumnsDescription & declared_columns);

}
