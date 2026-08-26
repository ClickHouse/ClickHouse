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
  * These two functions decide what a name means, and which names a type offers at all. They follow the
  * same rules, so a listing cannot contradict a read.
  */
namespace SubcolumnResolution
{

void forEachSubcolumn(const ISerialization::SubstreamData & data, const IDataType::SubcolumnCallback & callback);

std::unique_ptr<IDataType::SubcolumnInfo> findSubcolumn(
    std::string_view subcolumn_name,
    const ISerialization::SubstreamData & data,
    size_t initial_array_level,
    bool throw_if_null);

}

}
