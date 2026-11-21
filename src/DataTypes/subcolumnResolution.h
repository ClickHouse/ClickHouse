#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/** A column is stored as a set of substreams: tuple elements, array sizes, null maps, JSON paths and
  * so on, each written separately. Some of them are also offered to the user as a subcolumn, and
  * subcolumn names are flat - Array and Nullable pass the nested name through unchanged - so two
  * different substreams can end up claiming the same name. Array(Tuple(size0 UInt64)) is one example:
  * the array sizes and the tuple element are both called size0. Types like JSON add another source of
  * names, resolving them from the data as dynamic subcolumns, which collide with generated ones too.
  *
  * These functions decide what a name means, and which names a type offers at all. They follow the
  * same rules, so a listing cannot contradict a read.
  */
namespace SubcolumnResolution
{

using SubcolumnPathCallback = std::function<void(const IDataType::SubstreamPath &, const String &)>;

void forEachSubcolumn(
    const IDataType::SubstreamData & data, const SubcolumnPathCallback & callback, size_t initial_array_level = 0);
void forEachSubcolumn(
    const IDataType::SubstreamData & data, const IDataType::SubcolumnCallback & callback, size_t initial_array_level = 0);

/// With `allow_dynamic_subcolumns` false, only the subcolumns the type knows are resolved, none from the data.
std::unique_ptr<IDataType::SubcolumnInfo> findSubcolumn(
    std::string_view subcolumn_name,
    const IDataType::SubstreamData & data,
    size_t initial_array_level,
    bool throw_if_null,
    bool allow_dynamic_subcolumns = true);

}

}
