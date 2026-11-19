#pragma once

#include <DataTypes/Serializations/ISerialization.h>

#include <optional>

namespace DB
{

/// A stream of a column by its file name and, for a stream shared with other subcolumns, this column's bytes of it.
struct StreamWithSizeHint
{
    String name;
    std::optional<ISerialization::Substream::SizeHint> size_hint;
};

}
