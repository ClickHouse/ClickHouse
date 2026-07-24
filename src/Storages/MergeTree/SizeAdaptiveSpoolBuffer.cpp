#include <Storages/MergeTree/SizeAdaptiveSpoolBuffer.h>

#include <IO/PackedFilesWriter.h>
#include <Common/Exception.h>

#include <cstring>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

SizeAdaptiveSpoolBuffer::SizeAdaptiveSpoolBuffer(
    size_t spill_threshold_,
    size_t buf_size_,
    OpenPerFileFunc open_per_file_,
    PackedFilesWriter * packed_writer_,
    const String & packed_virtual_name_,
    const WriteSettings & packed_write_settings_,
    const String & display_file_name_,
    bool * coupled_spilled_flag_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , spill_threshold(spill_threshold_)
    , open_per_file(std::move(open_per_file_))
    , packed_writer(packed_writer_)
    , packed_virtual_name(packed_virtual_name_)
    , packed_write_settings(packed_write_settings_)
    , display_file_name(display_file_name_)
    , coupled_spilled_flag(coupled_spilled_flag_)
{
}

SizeAdaptiveSpoolBuffer::~SizeAdaptiveSpoolBuffer() = default;

void SizeAdaptiveSpoolBuffer::nextImpl()
{
    const size_t bytes_in_buffer = offset();
    if (bytes_in_buffer == 0)
        return;

    /// Peer (e.g. our marks file vs our data file) crossed the threshold first; we must spill
    /// too so the substream stays consistent (both packed or both per-file).
    if (!spilled && coupled_spilled_flag && *coupled_spilled_flag)
        promoteToPerFile();

    if (spilled)
    {
        per_file_writer->write(working_buffer.begin(), bytes_in_buffer);
        return;
    }

    /// Append to the in-memory accumulator. If we exceed the threshold, promote to per-file
    /// and dump everything we have so subsequent writes flow straight through.
    const size_t old_size = accumulator.size();
    accumulator.resize(old_size + bytes_in_buffer);
    std::memcpy(accumulator.data() + old_size, working_buffer.begin(), bytes_in_buffer);

    if (accumulator.size() > spill_threshold)
        promoteToPerFile();
}

void SizeAdaptiveSpoolBuffer::promoteToPerFile()
{
    per_file_writer = open_per_file();
    per_file_writer->write(reinterpret_cast<const char *>(accumulator.data()), accumulator.size());
    /// Free the accumulator memory: from now on we'll forward bytes straight to per_file_writer.
    PaddedPODArray<UInt8>{}.swap(accumulator);
    spilled = true;
    /// Tell our peer file (data file vs marks file of the same substream) to spill as well, so
    /// the on-disk substream stays in a single layout the reader can pick up.
    if (coupled_spilled_flag)
        *coupled_spilled_flag = true;
}

void SizeAdaptiveSpoolBuffer::commitToPackedIfNeeded()
{
    if (spilled || committed_to_packed)
        return;

    if (!packed_writer)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "SizeAdaptiveSpoolBuffer for '{}' has no packed writer at commit time", display_file_name);

    auto packed_buf = packed_writer->writeFile(packed_virtual_name, packed_write_settings);
    if (!accumulator.empty())
        packed_buf->write(reinterpret_cast<const char *>(accumulator.data()), accumulator.size());
    if (sync_requested)
        packed_buf->sync();
    packed_buf->finalize();

    PaddedPODArray<UInt8>{}.swap(accumulator);
    committed_to_packed = true;
}

void SizeAdaptiveSpoolBuffer::finalizeImpl()
{
    next();

    /// Last chance to honour a peer's late spill: if the data file spilled but the marks file
    /// had no more flushes between the spill and finalize, we still need to move marks to
    /// per-file before committing.
    if (!spilled && coupled_spilled_flag && *coupled_spilled_flag)
        promoteToPerFile();

    if (spilled)
    {
        if (sync_requested)
            per_file_writer->sync();
        per_file_writer->finalize();
    }
    else
    {
        commitToPackedIfNeeded();
    }
}

void SizeAdaptiveSpoolBuffer::cancelImpl() noexcept
{
    if (per_file_writer)
        per_file_writer->cancel();
}

void SizeAdaptiveSpoolBuffer::sync()
{
    /// We can't sync until we know whether we're per-file or packed. Record the request and
    /// honour it at finalize time, when we know which downstream owns the bytes.
    sync_requested = true;
    if (spilled && per_file_writer)
        per_file_writer->sync();
}

std::string SizeAdaptiveSpoolBuffer::getFileName() const
{
    return display_file_name;
}

}
