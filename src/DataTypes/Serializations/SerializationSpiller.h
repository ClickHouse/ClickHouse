#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/TemporaryDataOnDisk.h>

#include <functional>
#include <memory>
#include <optional>
#include <vector>

namespace DB
{

/// Spills data a serialization buffers during a part write to temporary files, and reads it back. A spilled segment is
/// bytes in the caller's format, which the caller writes and reads.
///
/// The server's temporary storage is written only within space reserved before the bytes, so that a lack of space is a
/// refusal before writing and does not lose what a file holds. One file takes the segments, its reservation growing by
/// RESERVE_STEP, or by a segment's bound when that is more, within the limits of the scopes; a refused extension
/// finishes the file, which stays readable, and the segment goes to a new file. After a refusal of a new file the
/// temporary storage is closed, and the segment and every later one go to one file on the part's disk, whose errors
/// fail the part write. An error that loses segments already spilled to the temporary storage propagates, or
/// with `throw_on_lost_data` unset is reported by the return values, with everything released.
class SerializationSpiller
{
public:
    /// `scope_tag` names the scopes of temporary data shared with the other spillers of the write that use it
    /// (`TemporaryDataScopeRequest::tag`). `max_temporary_data_size` limits the data in the temporary storage, 0 for no
    /// limit of its own. With `part_disk_only`, the temporary storage is not used.
    SerializationSpiller(
        GetTemporaryDataScope get_temporary_data_scope_,
        String scope_tag_,
        size_t max_temporary_data_size_,
        bool part_disk_only,
        bool throw_on_lost_data_);

    /// An opaque handle of a spilled segment, valid for the spiller that returned it.
    enum class SpilledSegmentId : size_t
    {
    };

    /// Writes the bytes of a segment; called again when a write is retried.
    using WriteSegment = std::function<void(WriteBuffer & out)>;
    /// Reads a segment of `size` bytes from `in`, positioned at its start.
    using ReadSegment = std::function<void(ReadBuffer & in, size_t size)>;

    /// Spills the `size` bytes `write_segment` writes as one segment. Returns nothing when spilled data was lost.
    std::optional<SpilledSegmentId> spill(size_t size, const WriteSegment & write_segment);

    /// Ends the spills: the segments are read only after it, and a file is finished at its first read.
    void finishSpilling();

    /// Reads the segment with `read_segment`; only after finishSpilling. An error of reading the segment loses the
    /// spilled data, any other error of `read_segment` propagates. Returns false when spilled data was lost.
    bool readSpilledSegment(SpilledSegmentId id, const ReadSegment & read_segment);

    /// Drops every file and returns their reservations.
    void release();

private:
    /// How far a reservation of the temporary storage runs ahead of the data: it bounds the space reserved and not used.
    static constexpr size_t RESERVE_STEP = 64 * 1024 * 1024;
    /// Retries of a reservation refused for a reason unrelated to space, before the temporary storage is closed.
    static constexpr size_t MAX_TRANSIENT_RESERVE_RETRIES = 3;

    /// Where a spilled segment is.
    struct SpilledSegmentLocation
    {
        TemporaryDataPlacement placement = TemporaryDataPlacement::TemporaryStorage;
        /// The index of the file among the temporary storage files; the part's disk has one file.
        size_t file_index = 0;
        /// Where the segment begins in the file.
        size_t offset_in_file = 0;
        size_t size = 0;
    };

    struct File
    {
        TemporaryDataBufferPtr buffer;
        /// Opened at the first read of a segment, after the file is finished.
        std::unique_ptr<TemporaryDataReadBuffer> reader;
        /// The compressed bytes reserved for a temporary storage file so far.
        size_t capacity = 0;
    };

    std::optional<SpilledSegmentLocation> trySpillToTemporaryStorage(size_t size, const WriteSegment & write_segment);
    SpilledSegmentLocation spillToPartDisk(size_t size, const WriteSegment & write_segment);
    /// Grows the reservation of the current temporary storage file by at least `needed` bytes. False on a refusal,
    /// after which the file takes no more segments.
    bool tryReserveMoreInCurrentFile(size_t needed);
    /// A new temporary storage file with a reservation that admits `segment_size` bytes, or nothing when the temporary
    /// storage cannot take the segment and is closed.
    std::optional<File> openTemporaryStorageFile(size_t segment_size);
    void finishCurrentTemporaryStorageFile();
    void closeTemporaryStorage(const char * reason);

    /// The reader of `file` positioned at `offset_in_file`, opened at the first call.
    TemporaryDataReadBuffer & getReader(File & file, size_t offset_in_file);

    /// Called from a catch block for an error that loses segments spilled to the temporary storage: rethrows unless
    /// `throw_on_lost_data` is unset.
    void onSpilledDataLost(const char * action);

    const GetTemporaryDataScope get_temporary_data_scope;
    const String scope_tag;
    const size_t max_temporary_data_size;
    const bool throw_on_lost_data;

    /// Indexed by `SpilledSegmentId`.
    std::vector<SpilledSegmentLocation> spilled_segments;

    /// Got at the first spill; null when the temporary storage is not available.
    TemporaryDataOnDiskScopePtr temporary_storage_scope;
    std::vector<File> temporary_storage_files;
    /// The temporary storage file the segments are written to, if any.
    std::optional<size_t> current_temporary_storage_file;
    bool temporary_storage_closed = false;

    /// Held while the part's disk has a file, which is removed with the scope's directory.
    TemporaryDataOnDiskScopePtr part_disk_scope;
    std::optional<File> part_disk_file;

    bool spilled_data_lost = false;
    /// Set by finishSpilling.
    bool spilling_finished = false;
};

}
