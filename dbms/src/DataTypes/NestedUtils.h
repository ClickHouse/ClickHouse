#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>


namespace DB
{

namespace Nested
{
    std::string concatenateName(const std::string & nested_table_name, const std::string & nested_field_name);

    std::pair<std::string, std::string> splitName(const std::string & name);

    /// Returns the prefix of the name to the first '.'. Or the name is unchanged if there is no dot.
    std::string extractTableName(const std::string & nested_name);

    /// Replace Array(Tuple(...)) columns to a multiple of Array columns in a form of `column_name.element_name`.
    NamesAndTypesList flatten(const NamesAndTypesList & names_and_types);
    Block flatten(const Block & block);

    /// Collect Array columns in a form of `column_name.element_name` to single Array(Tuple(...)) column.
    NamesAndTypesList collect(const NamesAndTypesList & names_and_types);
};

}
