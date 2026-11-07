#pragma once

#include <string>
#include <Core/Names.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

class Context;
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

/// Replaces each read of a column of `insertion_columns` that is not in `written_columns`, in the `DEFAULT`s of
/// `structure_hint`, with the value the insertion table computes for that column. Nothing is replaced when these `DEFAULT`s
/// or the values define an expression alias.
void inlineUnwrittenColumnsInStructureHint(
    ColumnsDescription & structure_hint, const ColumnsDescription & insertion_columns, const Names & written_columns,
    const ContextPtr & context);

/// Removes the `DEFAULT` of each column of `structure_hint` that reads a column of `insertion_columns` by a name the hint
/// gives to other insertion table columns, and of each column whose `DEFAULT` reads a column whose `DEFAULT` is removed.
void removeMisboundDefaultsFromStructureHint(
    ColumnsDescription & structure_hint, const NameToNameSetMap & insertion_columns_by_hint_column, const ColumnsDescription & insertion_columns);

/// Names and types of `columns`, plus each `DEFAULT` without secret arguments that the nodes can evaluate from them.
String getStructureToSend(const ColumnsDescription & columns, const ColumnsDescription * insertion_columns);

}
