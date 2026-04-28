#pragma once

#include <Core/Types.h>

#include <sys/types.h>

namespace DB
{

class WriteBufferFromFileBase;
class WriteBufferFromFileDescriptor;

/// Streaming serialiser for the DiskANN `.fbin` input format.
///
/// Layout:
///   header (8 bytes): UInt32 npoints, UInt32 ndim
///   body: npoints × ndim × float32 (row-major)
///
/// Because `npoints` is only known after the last row is appended, the writer reserves the
/// header with a placeholder, streams rows as they arrive, and seeks back to patch the final
/// count in `finalize`. The underlying buffer must therefore be seekable — the class checks
/// at construction time by `dynamic_cast`-ing to `WriteBufferFromFileDescriptor`.
class DiskANNFbinWriter
{
public:
    DiskANNFbinWriter(WriteBufferFromFileBase & buf_, UInt32 dim_);
    ~DiskANNFbinWriter();

    DiskANNFbinWriter(const DiskANNFbinWriter &) = delete;
    DiskANNFbinWriter & operator=(const DiskANNFbinWriter &) = delete;

    /// Append one row. `vec_dim` must equal the `dim` passed to the constructor.
    void appendRow(const float * vec, size_t vec_dim);

    /// Patch the header with the final `npoints` value. Must be called before the destructor
    /// runs, otherwise the file is left with a zero-valued `npoints` and a warning is logged.
    void finalize();

    UInt64 numRows() const { return count; }
    UInt32 dim() const { return vector_dim; }

private:
    WriteBufferFromFileBase & out;
    WriteBufferFromFileDescriptor & seekable_out;
    UInt32 vector_dim;
    UInt64 count = 0;
    bool finalized = false;
    off_t header_pos = 0;
};

}
