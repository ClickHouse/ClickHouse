#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <Storages/ColumnSize.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB
{

/// A stream of a column by its file name and, for a stream shared with other subcolumns, this column's bytes of it.
/// Several streams may be views of one file.
struct StreamWithSizeHint
{
    String name;
    std::optional<ISerialization::Substream::SizeHint> size_hint;
    /// The path a read opens the stream by, without the data of the read that listed it.
    ISerialization::SubstreamPath substream_path;
};

/// The streams a read of a subcolumn of a part touches, and their total size.
struct SubcolumnStreams
{
    ColumnSize size;
    std::vector<StreamWithSizeHint> streams;
};

using SubcolumnStreamsPtr = std::shared_ptr<const SubcolumnStreams>;

}
