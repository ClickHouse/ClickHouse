#pragma once

#include <cstddef>


namespace DB
{

/** A write buffer that can use the boundaries between the values written into it, e.g. the values of a `String`
  * column, which are written one after another without separators, while their sizes are written into another stream.
  * A compressing buffer passes them to the codec (see `ICompressionCodec::needsValueBoundaries`), so that it can
  * compress every value on its own (e.g. `FSST`), and the values can be checked in the compressed data later.
  *
  * The boundaries only make the compression better suited for that, the data is the same without them.
  */
class IWriteBufferWithValueBoundaries
{
public:
    virtual ~IWriteBufferWithValueBoundaries() = default;

    /// Whether the boundaries are used. If not, there is no need to report them.
    virtual bool needsValueBoundaries() const = 0;

    /// Reports that the position at `offset` in the working buffer is a boundary between two values.
    /// The offsets must not decrease until the working buffer is flushed.
    virtual void addValueBoundary(size_t offset) = 0;
};

}
