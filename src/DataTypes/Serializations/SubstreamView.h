#pragma once

#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

/// A stream living in a slice of a shared file, with its marks in a slice of a sibling file. A serialization sets it on
/// the element of the stream's path where the path inside the slice starts: the elements before it followed by
/// data_stream name the shared file, followed by marks_stream the marks file. Both byte ranges are block-aligned, so
/// each is read through a file view over its own bounds.
struct SerializationSubstream::StreamView
{
    /// Tells apart the views of one file in the reader's stream map, never part of a file name.
    String key;
    Type data_stream = Type::Regular;
    Type marks_stream = Type::Regular;
    size_t begin = 0; /// byte range of the stream inside the data file
    size_t end = 0;
    size_t marks_begin = 0; /// byte range of the marks slice inside the marks stream
    size_t marks_end = 0;
    size_t marks_count = 0; /// marks in the slice, compressed and non-adaptive: a mark per granule and the final mark
};

}
