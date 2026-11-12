#pragma once

#include <optional>
#include <string>
#include <Core/Names.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

class Context;
class ITableFunction;
struct Settings;

struct DataTypeValidationSettings
{
    DataTypeValidationSettings() = default;

    explicit DataTypeValidationSettings(const Settings & settings);

    bool allow_suspicious_low_cardinality_types = true;
    bool allow_suspicious_fixed_string_types = true;
    bool allow_suspicious_variant_types = true;
    bool validate_nested_types = true;
    bool enable_time_time64_type = true;
    bool enable_nullable_tuple_type = true;
};

void validateDataType(const DataTypePtr & type, const DataTypeValidationSettings & settings);

/// Parses a common argument for table functions such as table structure given in string
[[nodiscard]] ColumnsDescription parseColumnsListFromString(const std::string & structure, const ContextPtr & context);

bool tryParseColumnsListFromString(const std::string & structure, ColumnsDescription & columns, const ContextPtr & context, String & error);

/// Storage columns of `columns` read by the `DEFAULT` of `column`; a subcolumn read is a read of its storage column. The first
/// argument of `dictGet`/`joinGet` and the right operand of `IN` name a dictionary, table or set unless they are a column.
NameSet getStorageColumnsReadByDefault(const ColumnsDescription & columns, const ColumnDescription & column);

/// Removes the `DEFAULT` of each column of `structure_hint` that reads a column of `insertion_columns` by a name the hint
/// gives to other insertion table columns, and of each column whose `DEFAULT` reads a column whose `DEFAULT` is removed.
void removeMisboundDefaultsFromStructureHint(
    ColumnsDescription & structure_hint, const NameToNameSetMap & insertion_columns_by_hint_column, const ColumnsDescription & insertion_columns);

/// Name of the column of `table_function` that the identifier with `identifier_parts` from the `SELECT` list reads, to use it
/// in the structure hint. A compound identifier is resolved as a column with its full name if the source has one, otherwise
/// the first part is stripped if it is `table_function_alias`. Empty if the identifier does not read a top-level column of
/// `table_function`, like a subcolumn or a column of another table. The source columns are read once into `source_column_names`.
String getStructureHintColumnName(
    const Strings & identifier_parts,
    const String & table_function_alias,
    const ITableFunction & table_function,
    const ContextPtr & context,
    std::optional<NameSet> & source_column_names);

/// Names and types of `columns`, plus each `DEFAULT` without secret arguments that the nodes can evaluate from them.
String getStructureToSend(const ColumnsDescription & columns, const ColumnsDescription * insertion_columns);

}
