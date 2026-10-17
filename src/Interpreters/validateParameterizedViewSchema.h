#pragma once

#include <Core/NamesAndTypes.h>

#include <functional>

namespace DB
{

class ColumnsDescription;

/// Validates that the actual schema of a parameterized view (after parameter substitution)
/// matches its declared schema. Throws TYPE_MISMATCH if they differ.
/// `check_access_before_mismatch`, if set, is called right before throwing: the exception reveals
/// declared column names and types, so the caller can use it to throw `ACCESS_DENIED` instead
/// when the user is not allowed to see them.
void validateParameterizedViewSchema(
    const String & table_name,
    const NamesAndTypesList & actual_names_and_types,
    const ColumnsDescription & declared_columns,
    const std::function<void()> & check_access_before_mismatch = {});

}
