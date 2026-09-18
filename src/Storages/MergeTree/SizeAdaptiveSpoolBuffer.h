#pragma once

#include <Common/PODArray.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>

#include <functional>
#include <memory>

namespace DB
{

class PackedFilesWriter;

/** Write-buffer wrapper used by skip-index streams to decide *at write time* whether the
  * substream's bytes should land inside skp_idx.packed (the bundled archive) or as a
  * standalone per-file substream on the part's data storage.
  *
  * Behaviour:
  *   - Writes accumulate into an in-memory buffer (PaddedPODArray).
  *   - As long as the accumulator stays under @spill_threshold, the substream is a packed
  *     candidate: at finalize the bytes are streamed into a fresh @packed_writer entry.
  *   - The first nextImpl() that crosses @spill_threshold spills: a real WriteBuffer is
  *     opened via @open_per_file (e.g. data_part_storage->writeFile), the buffered bytes
  *     are dumped into it, and every subsequent flush forwards directly. From then on the
  *     substream is a standalone per-file artifact and will never enter the archive.
  *
  * The wrapper sits below the per-substream hashing/compression chain in MergeTreeWriterStream.
  * Because all bytes flow through the wrapper's working_buffer on their way down, the hashing
  * buffers above see exactly the bytes that end up on disk (whether packed or per-file), so
  * the per-substream checksums are correct in either branch.
  *
  * The caller queries @isSpilled() after preFinalize() to decide which checksum path to take:
  *   - spilled  -> per-file substream: emit name+extension and name+mrk_extension entries.
  *   - packed   -> the bytes are in @packed_writer; only the archive's single skp_idx.packed
  *                  checksum is emitted, by the writer that finalizes the archive.
  */
class SizeAdaptiveSpoolBuffer : public WriteBufferFromFileBase
{
public:
    using OpenPerFileFunc = std::function<std::unique_ptr<WriteBufferFromFileBase>()>;

    /// @coupled_spilled_flag: optional shared spill flag (lifetime owned by the caller). A
    /// substream's data file and marks file must travel together: either both are inside the
    /// archive or both are standalone per-file artifacts (the reader can't satisfy the
    /// half-and-half shape). Pass the same flag to both buffers so the first to cross the
    /// threshold forces the other to spill on its next flush (or at finalize). Pass nullptr
    /// when there's no peer to coordinate with.
    SizeAdaptiveSpoolBuffer(
        size_t spill_threshold_,
        size_t buf_size_,
        OpenPerFileFunc open_per_file_,
        PackedFilesWriter * packed_writer_,
        const String & packed_virtual_name_,
        const WriteSettings & packed_write_settings_,
        const String & display_file_name_,
        bool * coupled_spilled_flag_);

    ~SizeAdaptiveSpoolBuffer() override;

    void nextImpl() override;
    void finalizeImpl() override;
    void cancelImpl() noexcept override;
    void sync() override;
    std::string getFileName() const override;

    /// True iff this substream crossed @spill_threshold during writes and got promoted to a
    /// standalone per-file write. Defined after finalize() (or preFinalize for early peek).
    bool isSpilled() const { return spilled; }

private:
    void promoteToPerFile();
    void commitToPackedIfNeeded();

    const size_t spill_threshold;
    const OpenPerFileFunc open_per_file;
    PackedFilesWriter * const packed_writer;
    const String packed_virtual_name;
    const WriteSettings packed_write_settings;
    const String display_file_name;

    /// In-memory accumulator for the "still packable" phase.
    PaddedPODArray<UInt8> accumulator;

    /// Set once the substream has been promoted to a standalone file.
    bool spilled = false;
    std::unique_ptr<WriteBufferFromFileBase> per_file_writer;

    /// True after the accumulator has been handed to packed_writer at finalize time.
    bool committed_to_packed = false;

    /// Propagates a caller's sync() request: if not spilled, sync the packed archive when it
    /// gets written; if spilled, sync the per-file writer directly.
    bool sync_requested = false;

    /// See constructor docs. Non-owning.
    bool * coupled_spilled_flag = nullptr;
};

}
