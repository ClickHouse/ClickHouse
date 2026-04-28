#include <Storages/MergeTree/ANNIndex/DiskANNFbinWriter.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{
    WriteBufferFromFileDescriptor & requireSeekable(WriteBufferFromFileBase & buf)
    {
        auto * seekable = dynamic_cast<WriteBufferFromFileDescriptor *>(&buf);
        if (!seekable)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "DiskANNFbinWriter requires a seekable WriteBufferFromFileDescriptor; got {}",
                buf.getFileName());
        return *seekable;
    }
}

DiskANNFbinWriter::DiskANNFbinWriter(WriteBufferFromFileBase & buf_, UInt32 dim_)
    : out(buf_)
    , seekable_out(requireSeekable(buf_))
    , vector_dim(dim_)
{
    if (vector_dim == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DiskANNFbinWriter: dim must be > 0");

    /// Remember where the header lives so `finalize` can seek back to it.
    /// Flush whatever is already pending so the recorded offset is the physical byte position.
    out.next();
    header_pos = seekable_out.seek(0, SEEK_CUR);

    /// Reserve the header slot with a placeholder count; `finalize` overwrites this.
    const UInt32 placeholder_npoints = 0;
    writePODBinary(placeholder_npoints, out);
    writePODBinary(vector_dim, out);
}

DiskANNFbinWriter::~DiskANNFbinWriter()
{
    if (!finalized)
    {
        /// We purposefully don't throw from the destructor. Callers that care must call
        /// `finalize`; leaving the file with `npoints = 0` is at least a well-defined state
        /// (the background cleanup path will remove the tmp group directory).
        try
        {
            auto log = getLogger("DiskANNFbinWriter");
            LOG_WARNING(log,
                "DiskANNFbinWriter destroyed without finalize; file `{}` has npoints = 0",
                out.getFileName());
        }
        catch (...)
        {
            /// Best-effort logging.
        }
    }
}

void DiskANNFbinWriter::appendRow(const float * vec, size_t vec_dim)
{
    if (finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANNFbinWriter::appendRow called after `finalize`");
    if (vec_dim != vector_dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DiskANNFbinWriter::appendRow: vector dim mismatch (got {}, expected {})",
            vec_dim, vector_dim);

    out.write(reinterpret_cast<const char *>(vec),
              static_cast<size_t>(vector_dim) * sizeof(float));
    ++count;
}

void DiskANNFbinWriter::finalize()
{
    if (finalized)
        return;

    /// Ensure every pending row has reached the backing fd before we seek.
    out.next();

    const off_t tail_pos = seekable_out.seek(0, SEEK_CUR);

    if (count > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANNFbinWriter: too many rows ({}), exceeds UInt32 max", count);

    /// Patch the header: both fields are 4 bytes each at `header_pos`.
    seekable_out.seek(header_pos, SEEK_SET);
    const UInt32 npoints = static_cast<UInt32>(count);
    writePODBinary(npoints, out);
    writePODBinary(vector_dim, out);
    out.next();

    /// Return to the tail so subsequent writes (if any) append rather than overwrite.
    seekable_out.seek(tail_pos, SEEK_SET);

    finalized = true;
}

}
