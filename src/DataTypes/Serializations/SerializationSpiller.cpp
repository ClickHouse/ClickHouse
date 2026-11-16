#include <DataTypes/Serializations/SerializationSpiller.h>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event SerializationSpilledSegments;
    extern const Event SerializationSpilledBytes;
    extern const Event SerializationSpilledUncompressedBytes;
    extern const Event SerializationSpillFiles;
    extern const Event SerializationSpilledToPartDisk;
    extern const Event SerializationSpillErrors;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int FAULT_INJECTED;
    extern const int LOGICAL_ERROR;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int NOT_ENOUGH_SPACE;
    extern const int QUERY_WAS_CANCELLED;
}

namespace FailPoints
{
    extern const char serialization_spill_fail_write[];
    extern const char serialization_spill_fail_part_disk[];
    extern const char serialization_spill_read_eof[];
    extern const char serialization_spill_reserve_more_refused[];
}

SerializationSpiller::SerializationSpiller(
    GetTemporaryDataScope get_temporary_data_scope_,
    String scope_tag_,
    size_t max_temporary_data_size_,
    bool part_disk_only,
    bool throw_on_lost_data_)
    : get_temporary_data_scope(std::move(get_temporary_data_scope_))
    , scope_tag(std::move(scope_tag_))
    , max_temporary_data_size(max_temporary_data_size_)
    , throw_on_lost_data(throw_on_lost_data_)
    , temporary_storage_closed(part_disk_only)
{
}

std::optional<SerializationSpiller::SpilledSegmentId>
SerializationSpiller::spill(size_t size, const WriteSegment & write_segment)
{
    if (spilled_data_lost)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Spilled serialization data is used after it was lost");
    if (spilling_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Serialization data is spilled after the spilling was finished");

    std::optional<SpilledSegmentLocation> location;
    if (!temporary_storage_closed)
    {
        location = trySpillToTemporaryStorage(size, write_segment);
        if (spilled_data_lost)
            return std::nullopt;
    }
    if (!location)
        location = spillToPartDisk(size, write_segment);

    spilled_segments.push_back(*location);
    return static_cast<SpilledSegmentId>(spilled_segments.size() - 1);
}

namespace
{

/// Whether the current exception may be reported as lost spilled data; the rest are not about the spilled data and
/// always propagate.
bool canReportLostData()
{
    try
    {
        throw;
    }
    catch (const std::bad_alloc &)
    {
        return false;
    }
    catch (...) // Ok: the caller rethrows or logs the exception.
    {
        int code = getCurrentExceptionCode();
        return code != ErrorCodes::LOGICAL_ERROR && code != ErrorCodes::MEMORY_LIMIT_EXCEEDED && code != ErrorCodes::ABORTED
            && code != ErrorCodes::QUERY_WAS_CANCELLED;
    }
}

LoggerPtr getSpillerLogger()
{
    return getLogger("SerializationSpiller");
}

void writeSegment(TemporaryDataBuffer & buffer, size_t size, const SerializationSpiller::WriteSegment & write_segment)
{
    const size_t count_before = buffer.count();
    write_segment(buffer);
    /// The segment's bound, which the reservation admits, is of its declared size.
    if (buffer.count() - count_before != size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A spilled segment of {} bytes was written as {} bytes", size, buffer.count() - count_before);
    ProfileEvents::increment(ProfileEvents::SerializationSpilledSegments);
}

}

std::optional<SerializationSpiller::SpilledSegmentLocation>
SerializationSpiller::trySpillToTemporaryStorage(size_t size, const WriteSegment & write_segment)
{
    if (current_temporary_storage_file)
    {
        auto & file = temporary_storage_files[*current_temporary_storage_file];
        try
        {
            /// Flushes the previous segment, so that this one starts a fresh block and its bound holds.
            size_t offset_in_file = file.buffer->startNewBlock();
            const size_t end_bound = offset_in_file + temporary_storage_scope->getCompressedBound(size);
            if (end_bound <= file.capacity || tryReserveMoreInCurrentFile(end_bound - file.capacity))
            {
                fiu_do_on(FailPoints::serialization_spill_fail_write,
                {
                    throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure of a write to the temporary storage");
                });
                writeSegment(*file.buffer, size, write_segment);
                return SpilledSegmentLocation{
                    .placement = TemporaryDataPlacement::TemporaryStorage,
                    .file_index = *current_temporary_storage_file,
                    .offset_in_file = offset_in_file,
                    .size = size};
            }
            finishCurrentTemporaryStorageFile();
        }
        catch (...)
        {
            /// Ok: onSpilledDataLost rethrows the error, or logs it and reports the lost data.
            onSpilledDataLost("writing to a file of the temporary storage");
            return std::nullopt;
        }
    }

    auto file = openTemporaryStorageFile(size);
    if (!file)
        return std::nullopt;

    /// The new file holds nothing yet, so an error here loses no segment: the segment goes to the part's disk.
    size_t offset_in_file = 0;
    try
    {
        offset_in_file = file->buffer->startNewBlock();
        writeSegment(*file->buffer, size, write_segment);
    }
    catch (...)
    {
        if (!canReportLostData())
            throw;
        tryLogCurrentException(getSpillerLogger(), "Cannot write to a new file of the temporary storage");
        closeTemporaryStorage("a write to a new file failed");
        return std::nullopt;
    }

    temporary_storage_files.push_back(std::move(*file));
    current_temporary_storage_file = temporary_storage_files.size() - 1;
    ProfileEvents::increment(ProfileEvents::SerializationSpillFiles);
    return SpilledSegmentLocation{
        .placement = TemporaryDataPlacement::TemporaryStorage,
        .file_index = *current_temporary_storage_file,
        .offset_in_file = offset_in_file,
        .size = size};
}

std::optional<SerializationSpiller::File> SerializationSpiller::openTemporaryStorageFile(size_t segment_size)
{
    if (!temporary_storage_scope)
    {
        /// The distributed cache cannot reserve space and may drop what a connection idle for long holds, which a
        /// segment spilled early and read at the end of a long write is.
        temporary_storage_scope = get_temporary_data_scope(TemporaryDataScopeRequest{
            .placement = TemporaryDataPlacement::TemporaryStorage,
            .max_size = max_temporary_data_size,
            .allowed_backends = makeTemporaryDataBackends({TemporaryDataBackend::Disk, TemporaryDataBackend::LocalCache}),
            .tag = scope_tag,
            .bytes_compressed = ProfileEvents::SerializationSpilledBytes,
            .bytes_uncompressed = ProfileEvents::SerializationSpilledUncompressedBytes});
        if (!temporary_storage_scope)
        {
            closeTemporaryStorage("it is not available");
            return std::nullopt;
        }
    }

    /// The file holds at least the segment it is opened for, and is reserved a step ahead of it within the limits.
    const size_t needed = temporary_storage_scope->getCompressedBound(segment_size);
    size_t to_reserve = std::max(needed, RESERVE_STEP);
    if (auto remaining = temporary_storage_scope->getRemainingSize())
    {
        if (*remaining < needed)
        {
            closeTemporaryStorage("its limits leave less than the segment");
            return std::nullopt;
        }
        to_reserve = std::min(to_reserve, *remaining);
    }

    size_t transient_retries = 0;
    while (true)
    {
        ReservedTemporaryDataBuffer reserved;
        try
        {
            reserved = TemporaryDataBuffer::tryCreateWithReservation(temporary_storage_scope, to_reserve);
        }
        catch (...)
        {
            if (!canReportLostData())
                throw;
            tryLogCurrentException(getSpillerLogger(), "Cannot create a file in the temporary storage");
            closeTemporaryStorage("a file cannot be created");
            return std::nullopt;
        }

        if (reserved.buffer)
            return File{.buffer = std::move(reserved.buffer), .reader = {}, .capacity = to_reserve};

        if (reserved.result == TemporaryDataReserveResult::Transient)
        {
            if (transient_retries < MAX_TRANSIENT_RESERVE_RETRIES)
            {
                ++transient_retries;
                continue;
            }
            /// A smaller reservation would meet the same refusal, which is not about space.
            closeTemporaryStorage("the reservation of a file is refused for a reason other than space");
            return std::nullopt;
        }

        /// The step ahead is best-effort: the segment alone may still fit.
        if (to_reserve == needed)
        {
            closeTemporaryStorage("there is no space for a file");
            return std::nullopt;
        }
        to_reserve = needed;
    }
}

bool SerializationSpiller::tryReserveMoreInCurrentFile(size_t needed)
{
    auto & file = temporary_storage_files[*current_temporary_storage_file];
    /// A refusal of the backend may make the file unwritable, so every byte is written to it before.
    file.buffer->flushToFile();

    size_t to_reserve = std::max(needed, RESERVE_STEP);
    if (auto remaining = temporary_storage_scope->getRemainingSize())
    {
        if (*remaining < needed)
            return false;
        to_reserve = std::min(to_reserve, *remaining);
    }

    fiu_do_on(FailPoints::serialization_spill_reserve_more_refused,
    {
        LOG_DEBUG(getSpillerLogger(), "Injected refusal to extend the reservation of a file of the temporary storage");
        return false;
    });

    /// Not retried on this file: after a refusal of the cache its segment takes no more reservations. The caller opens a
    /// new file, where the step ahead is dropped if only the segment fits.
    if (file.buffer->tryReserveMore(to_reserve) != TemporaryDataReserveResult::Reserved)
        return false;

    file.capacity += to_reserve;
    return true;
}

void SerializationSpiller::finishCurrentTemporaryStorageFile()
{
    temporary_storage_files[*current_temporary_storage_file].buffer->finishWriting();
    current_temporary_storage_file.reset();
}

SerializationSpiller::SpilledSegmentLocation
SerializationSpiller::spillToPartDisk(size_t size, const WriteSegment & write_segment)
{
    if (!part_disk_file)
    {
        part_disk_scope = get_temporary_data_scope(TemporaryDataScopeRequest{
            .placement = TemporaryDataPlacement::PartDisk,
            .max_size = 0,
            .allowed_backends = {},
            .tag = scope_tag,
            .bytes_compressed = ProfileEvents::SerializationSpilledBytes,
            .bytes_uncompressed = ProfileEvents::SerializationSpilledUncompressedBytes});
        if (!part_disk_scope)
            throw Exception(ErrorCodes::NOT_ENOUGH_SPACE,
                "Cannot spill serialization data: the temporary storage cannot take it and the part's disk is not available");
        part_disk_file.emplace(File{.buffer = std::make_unique<TemporaryDataBuffer>(part_disk_scope), .reader = {}, .capacity = 0});
        ProfileEvents::increment(ProfileEvents::SerializationSpillFiles);
    }

    auto & buffer = *part_disk_file->buffer;
    size_t offset_in_file = buffer.startNewBlock();
    fiu_do_on(FailPoints::serialization_spill_fail_part_disk,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure of a write to the part's disk");
    });
    writeSegment(buffer, size, write_segment);
    ProfileEvents::increment(ProfileEvents::SerializationSpilledToPartDisk);
    return SpilledSegmentLocation{
        .placement = TemporaryDataPlacement::PartDisk, .file_index = 0, .offset_in_file = offset_in_file, .size = size};
}

TemporaryDataReadBuffer & SerializationSpiller::getReader(File & file, size_t offset_in_file)
{
    if (!file.reader)
        file.reader = file.buffer->readSeekable();
    file.reader->seekToBlock(offset_in_file);
    return *file.reader;
}

namespace
{

/// Reads a spilled segment and remembers whether reading failed, so that another error of the segment's reader is told
/// apart from the loss of the spilled data.
class SpilledSegmentReadBuffer : public ReadBuffer
{
public:
    explicit SpilledSegmentReadBuffer(ReadBuffer & in_)
        : ReadBuffer(nullptr, 0)
        , in(in_)
    {
        BufferBase::set(in.buffer().begin(), in.buffer().size(), in.offset());
        /// The file ends at the segment's start, as an empty or truncated one does.
        fiu_do_on(FailPoints::serialization_spill_read_eof,
        {
            BufferBase::set(in.position(), 0, 0);
            eof_injected = true;
        });
    }

    bool hasFailed() const { return failed; }

private:
    bool nextImpl() override
    {
        /// A segment lies inside its file and is read up to its size, so the end of the file is the source failing.
        if (eof_injected)
        {
            failed = true;
            return false;
        }

        in.position() = position();
        try
        {
            if (!in.next())
            {
                failed = true;
                return false;
            }
        }
        catch (...)
        {
            failed = true;
            throw;
        }
        BufferBase::set(in.buffer().begin(), in.buffer().size(), in.offset());
        return true;
    }

    ReadBuffer & in;
    bool failed = false;
    bool eof_injected = false;
};

}

bool SerializationSpiller::readSpilledSegment(SpilledSegmentId id, const ReadSegment & read_segment)
{
    if (spilled_data_lost)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Spilled serialization data is used after it was lost");

    /// A segment id stays valid until release, which drops every segment.
    if (static_cast<size_t>(id) >= spilled_segments.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Spilled segment {} is used after the spilled data was released", static_cast<size_t>(id));
    if (!spilling_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A spilled segment is read before the spilling was finished");
    const auto & location = spilled_segments[static_cast<size_t>(id)];

    /// Every error of the part's disk fails the part write.
    if (location.placement == TemporaryDataPlacement::PartDisk)
    {
        read_segment(getReader(*part_disk_file, location.offset_in_file), location.size);
        return true;
    }

    std::optional<SpilledSegmentReadBuffer> in;
    try
    {
        /// Nothing is spilled after finishSpilling, so the current file is finished at its first read.
        if (current_temporary_storage_file == location.file_index)
            finishCurrentTemporaryStorageFile();
        in.emplace(getReader(temporary_storage_files.at(location.file_index), location.offset_in_file));
    }
    catch (...)
    {
        /// Ok: onSpilledDataLost rethrows the error, or logs it and reports the lost data.
        onSpilledDataLost("opening a file of the temporary storage");
        return false;
    }

    try
    {
        read_segment(*in, location.size);
        return true;
    }
    catch (...)
    {
        bool reading_failed = in->hasFailed();
        in.reset();
        if (!reading_failed)
            throw;
        onSpilledDataLost("reading a file of the temporary storage");
        return false;
    }
}

void SerializationSpiller::closeTemporaryStorage(const char * reason)
{
    if (!temporary_storage_closed)
        LOG_DEBUG(getSpillerLogger(), "The temporary storage is closed for spilling, as {}; the next segments go to the part's disk", reason);
    temporary_storage_closed = true;
}

void SerializationSpiller::onSpilledDataLost(const char * action)
{
    if (throw_on_lost_data || !canReportLostData())
        throw;

    tryLogCurrentException(getSpillerLogger(), fmt::format("Spilled serialization data was lost while {}", action));
    ProfileEvents::increment(ProfileEvents::SerializationSpillErrors);
    release();
    spilled_data_lost = true;
}

void SerializationSpiller::finishSpilling()
{
    spilling_finished = true;
}

void SerializationSpiller::release()
{
    /// A file's reader is destroyed before its buffer, which removes the file.
    temporary_storage_files.clear();
    current_temporary_storage_file.reset();
    temporary_storage_scope.reset();
    part_disk_file.reset();
    part_disk_scope.reset();
    spilled_segments.clear();
}

}
