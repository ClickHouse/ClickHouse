#include <Common/UnorderedSetWithMemoryTracking.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteSettings.h>
#include <IO/WriteHelpers.h>
#include <IO/PackedFilesWriter.h>
#include <IO/copyData.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/escapeForFileName.h>

#include <algorithm>


namespace CurrentMetrics
{
    extern const Metric PackedPartsBufferedBytes;
}

namespace ProfileEvents
{
    extern const Event ExternalPackedPartWriterWritePart;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
}

PackedFilesWriter::PackedFilesWriter()
    : state(std::make_shared<SharedState>())
{
    state->writer = this;
}

PackedFilesWriter::~PackedFilesWriter()
{
    state->writer = nullptr;
}

void PackedFilesWriter::setSpillSettings(SpillSettings spill_settings_)
{
    if (spill_settings_.max_bytes_in_memory && !spill_settings_.create_temporary_file)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Spilling of packed files is enabled, but the temporary file cannot be created");

    spill_settings = std::move(spill_settings_);
    spillIfNeeded();
}

std::unique_ptr<WriteBufferFromFileBase>
PackedFilesWriter::writeFile(const String & file_name, const WriteSettings & settings)
{
    if (!write_settings && settings != WriteSettings{})
        write_settings = settings;

    return writeFile(file_name);
}

std::unique_ptr<WriteBufferFromFileBase>
PackedFilesWriter::writeFile(const String & file_name)
{
    auto [it, inserted] = written_files.try_emplace(file_name);

    if (!inserted)
    {
        String existing_files;
        for (const auto & [name, _] : written_files)
        {
            if (!existing_files.empty()) existing_files += ", ";
            existing_files += name;
        }
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
            "File {} already exists in packed archive (existing files: [{}])", file_name, existing_files);
    }

    it->second = std::make_shared<Data>(state);
    return std::make_unique<FakeWriteBufferFromFile>(file_name, it->second);
}

std::unique_ptr<WriteBufferFromFileBase>
PackedFilesWriter::rewriteFile(const String & file_name, const WriteSettings & settings)
{
    if (!write_settings && settings != WriteSettings{})
        write_settings = settings;

    /// A buffer of the replaced file may still be alive, but it owns its data and does not affect the new file.
    auto & data = written_files[file_name];
    data = std::make_shared<Data>(state);
    return std::make_unique<FakeWriteBufferFromFile>(file_name, data);
}

void PackedFilesWriter::moveFile(const String & from_name, const String & to_name)
{
    metadata_changes.emplace_back(MetadataChange::MOVE, from_name, to_name);
    applyMoveFile(metadata_changes.back(), written_files);
}

void PackedFilesWriter::replaceFile(const String & from_name, const String & to_name)
{
    metadata_changes.emplace_back(MetadataChange::REPLACE, from_name, to_name);
    applyMoveFile(metadata_changes.back(), written_files);
}

void PackedFilesWriter::removeFile(const String & name)
{
    metadata_changes.emplace_back(MetadataChange::REMOVE, name, "");
    applyRemoveFile(metadata_changes.back(), written_files);
}

void PackedFilesWriter::removeFileIfExists(const String & name)
{
    metadata_changes.emplace_back(MetadataChange::REMOVE_IF_EXISTS, name, "");
    applyRemoveFile(metadata_changes.back(), written_files);
}

void PackedFilesWriter::setUncompressedSize(const String & file_name, UInt64 uncompressed_size)
{
    auto it = written_files.find(file_name);
    if (it == written_files.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
            "Cannot set uncompressed size for {}. File does not exist in packed archive", file_name);
    it->second->uncompressed_size = uncompressed_size;
}

/// Returns the size of string written by @writeStringBinary call.
static UInt64 getLengthOfSerializedString(const String & str)
{
    return getLengthOfVarUInt(str.size()) + str.size();
}

void PackedFilesWriter::applyMetadataChanges(PackedFilesIO::Index & index)
{
    for (auto & change : metadata_changes)
    {
        switch (change.type)
        {
            case MetadataChange::MOVE:
            case MetadataChange::REPLACE:
                applyMoveFile(change, index);
                break;
            case MetadataChange::REMOVE:
            case MetadataChange::REMOVE_IF_EXISTS:
                applyRemoveFile(change, index);
                break;
        }
    }
}

template <typename Map>
void PackedFilesWriter::applyMoveFile(MetadataChange & change, Map & index_map)
{
    if (change.type == MetadataChange::MOVE && index_map.contains(change.to))
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
            "Cannot move file from {} to {}. File {} already exists", change.from, change.to, change.to);

    auto it = index_map.find(change.from);
    if (it != index_map.end())
    {
        index_map.erase(change.to);
        change.is_applied = true;

        auto entry = index_map.extract(it);
        entry.key() = change.to;
        index_map.insert(std::move(entry));
    }
}

template <typename Map>
void PackedFilesWriter::applyRemoveFile(MetadataChange & change, Map & index_map)
{
    auto it = index_map.find(change.from);
    if (it != index_map.end())
    {
        index_map.erase(it);
        change.is_applied = true;
    }
}


void PackedFilesWriter::writePackedIndex(WriteBuffer & out, const PackedFilesIO::Index & index, UInt8 version)
{
    writeIntBinary(version, out);
    writeIntBinary(index.size(), out);

    for (const auto & [name, offset] : index)
    {
        writeStringBinary(name, out);
        writeIntBinary(offset.offset, out);
        writeIntBinary(offset.size, out);
        if (version >= PackedFilesIO::VERSION_WITH_UNCOMPRESSED_SIZE)
            writeIntBinary(offset.uncompressed_size, out);
    }
}

PackedFilesWriter::FinalizePlan PackedFilesWriter::prepareFinalize(const Strings & files_order_hint, UInt8 version) const
{
    const bool with_uncompressed_size = version >= PackedFilesIO::VERSION_WITH_UNCOMPRESSED_SIZE;
    for (const auto & change : metadata_changes)
    {
        if (!change.is_applied)
        {
            if (change.type == MetadataChange::MOVE || change.type == MetadataChange::REPLACE)
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
                    "Cannot move file from {} to {}. File {} does not exist", change.from, change.to, change.from);

            if (change.type == MetadataChange::REMOVE)
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
                    "Cannot remove file {}. File does not exist", change.from);
        }
    }

    const UInt64 num_files = written_files.size();

    Strings ordered_file_names;
    ordered_file_names.reserve(num_files);
    /// Order files according to the hint.
    {
        UnorderedSetWithMemoryTracking<String> already_added_files;
        for (const auto & hinted_name : files_order_hint)
        {
            std::string found_file_name;
            bool found = false;
            for (const auto & [key, _] : written_files)
            {
                if (unescapeForFileName(key) == hinted_name)
                {
                    found_file_name = key;
                    found = true;
                    break;
                }
            }

            if (found && !already_added_files.contains(found_file_name))
            {
                ordered_file_names.push_back(found_file_name);
                already_added_files.insert(found_file_name);
            }
        }
        for (const auto & [name, _] : written_files)
        {
            if (!already_added_files.contains(name))
            {
                ordered_file_names.push_back(name);
                already_added_files.insert(name);
            }
        }
    }
    chassert(ordered_file_names.size() == num_files, "Number of files in ordered list doesn't match the number of written files");

    /// Calculate the size of index.
    /// Per-file fields: file_name, offset, size [, uncompressed_size in v1+].
    const UInt64 num_size_fields = with_uncompressed_size ? 3 : 2;
    UInt64 data_offset = getSizeOfHeader();
    for (const auto & name : ordered_file_names)
        data_offset += getLengthOfSerializedString(name) + sizeof(UInt64) * num_size_fields;

    PackedFilesIO::Index index;
    bool need_sync = false;
    for (const auto & name : ordered_file_names)
    {
        const auto & data = written_files.at(name);
        if (!data->is_finalized)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "File {} is not finalized before finalization of packed archive", name);

        const UInt64 data_size = data->size;

        index[name] = {data_offset, data_size, data->uncompressed_size};
        data_offset += data_size;
        /// fsync the whole file with archive if any of files were requested to be fsynced.
        need_sync |= data->need_sync;
    }

    return {std::move(index), std::move(ordered_file_names), version, need_sync};
}

void PackedFilesWriter::finalize(WriteBuffer & out, const FinalizePlan & plan) const
{
    const bool with_uncompressed_size = plan.version >= PackedFilesIO::VERSION_WITH_UNCOMPRESSED_SIZE;
    const UInt64 num_files = plan.ordered_file_names.size();

    writeIntBinary(plan.version, out);
    writeIntBinary(num_files, out);

    for (const auto & name : plan.ordered_file_names)
    {
        const auto & offset = plan.index.at(name);
        writeStringBinary(name, out);
        writeIntBinary(offset.offset, out);
        writeIntBinary(offset.size, out);
        if (with_uncompressed_size)
            writeIntBinary(offset.uncompressed_size, out);
    }

    /// The data cannot be moved out of memory anymore: the temporary file is read only after all writes to it.
    if (temporary_file)
        temporary_file->finishWriting();

    for (const auto & name : plan.ordered_file_names)
    {
        const auto & data = written_files.at(name);
        UInt64 written_size = 0;

        for (const auto & range : data->ranges_in_temporary_file)
        {
            auto in = temporary_file->readRange(range.position);
            copyData(*in, out, range.size);
            written_size += range.size;
        }

        for (size_t i = 0; i < data->chunks.size(); ++i)
        {
            const size_t chunk_data_size = data->getChunkDataSize(i);
            out.write(data->chunks[i].data(), chunk_data_size);
            written_size += chunk_data_size;
        }

        if (written_size != plan.index.at(name).size)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Wrote {} bytes of file {} into packed archive, expected {}", written_size, name, plan.index.at(name).size);
    }
}

std::pair<PackedFilesIO::Index, bool> PackedFilesWriter::finalize(WriteBuffer & out, const Strings & files_order_hint, UInt8 version) const
{
    auto plan = prepareFinalize(files_order_hint, version);
    finalize(out, plan);
    return {std::move(plan.index), plan.need_sync};
}

size_t PackedFilesWriter::getSizeOfHeader()
{
    /// 2 fields: version (UInt8), number of files (UInt64).
    return sizeof(UInt8) + sizeof(UInt64);
}

void PackedFilesWriter::spillIfNeeded()
{
    if (!spill_settings.max_bytes_in_memory || state->bytes_in_memory <= spill_settings.max_bytes_in_memory)
        return;

    /// The last chunks of the files being written can take more than the limit (e.g. a wide part with many columns).
    /// Then the limit cannot be reached, so wait until there is at least half of it to move out of memory at once:
    /// otherwise every new chunk would be moved separately, as a new range of the temporary file.
    if (state->bytes_in_memory - state->non_spillable_bytes_in_memory < spill_settings.max_bytes_in_memory / 2)
        return;

    spill();
}

void PackedFilesWriter::spill()
{
    /// Free half of the limit at once, so that a writer that keeps receiving data
    /// does not move it out of memory by small pieces on every new chunk.
    const size_t target_bytes_in_memory = spill_settings.max_bytes_in_memory / 2;

    struct Candidate
    {
        Data * data;
        size_t num_chunks;
        size_t bytes;
    };

    VectorWithMemoryTracking<Candidate> candidates;
    for (const auto & [_, data] : written_files)
    {
        const size_t num_chunks = data->getNumSpillableChunks();
        size_t bytes = 0;
        for (size_t i = 0; i < num_chunks; ++i)
            bytes += data->chunks[i].size();

        if (bytes)
            candidates.push_back({data.get(), num_chunks, bytes});
    }

    if (candidates.empty())
        return;

    /// Move the largest files first to get fewer ranges to read back.
    std::sort(candidates.begin(), candidates.end(), [](const auto & lhs, const auto & rhs) { return lhs.bytes > rhs.bytes; });

    /// All moves append to the same temporary file, which is read back only by @finalize.
    if (!temporary_file)
        temporary_file = spill_settings.create_temporary_file();

    ProfileEvents::increment(ProfileEvents::ExternalPackedPartWriterWritePart);

    for (const auto & candidate : candidates)
    {
        if (state->bytes_in_memory <= target_bytes_in_memory)
            break;

        Data & data = *candidate.data;
        RangeInTemporaryFile range{.position = temporary_file->startRange(), .size = 0};

        for (size_t i = 0; i < candidate.num_chunks; ++i)
        {
            const size_t chunk_data_size = data.getChunkDataSize(i);
            temporary_file->write(data.chunks[i].data(), chunk_data_size);
            range.size += chunk_data_size;
        }

        if (range.size)
            data.ranges_in_temporary_file.push_back(range);
        data.removeChunks(candidate.num_chunks);
    }
}

PackedFilesWriter::Data::Data(std::shared_ptr<SharedState> state_)
    : state(std::move(state_))
{
}

PackedFilesWriter::Data::~Data()
{
    size_t bytes = 0;
    for (const auto & chunk : chunks)
        bytes += chunk.size();

    state->bytes_in_memory -= bytes;
    if (!is_finalized && !chunks.empty())
        state->non_spillable_bytes_in_memory -= chunks.back().size();
    CurrentMetrics::sub(CurrentMetrics::PackedPartsBufferedBytes, bytes);
}

void PackedFilesWriter::Data::addChunk()
{
    /// The previous last chunk is filled entirely now, so it can be moved out of memory.
    if (!chunks.empty())
        state->non_spillable_bytes_in_memory -= chunks.back().size();

    chunks.emplace_back(next_chunk_size);
    state->bytes_in_memory += next_chunk_size;
    state->non_spillable_bytes_in_memory += next_chunk_size;
    CurrentMetrics::add(CurrentMetrics::PackedPartsBufferedBytes, next_chunk_size);
    next_chunk_size = std::min(next_chunk_size * 2, MAX_CHUNK_SIZE);

    /// Check the limit whenever the memory grows, including the first chunk of a new file.
    /// The new chunk is the last one of a file being written, so it stays in memory.
    if (auto * writer = state->writer)
        writer->spillIfNeeded();
}

size_t PackedFilesWriter::Data::getChunkDataSize(size_t chunk_index) const
{
    if (chunk_index + 1 < chunks.size())
        return chunks[chunk_index].size();

    chassert(is_finalized);
    return last_chunk_data_size;
}

size_t PackedFilesWriter::Data::getNumSpillableChunks() const
{
    if (is_finalized || chunks.empty())
        return chunks.size();
    return chunks.size() - 1;
}

void PackedFilesWriter::Data::removeChunks(size_t num_chunks)
{
    chassert(num_chunks <= chunks.size());
    size_t bytes = 0;
    for (size_t i = 0; i < num_chunks; ++i)
        bytes += chunks[i].size();

    state->bytes_in_memory -= bytes;
    CurrentMetrics::sub(CurrentMetrics::PackedPartsBufferedBytes, bytes);
    chunks.erase(chunks.begin(), chunks.begin() + num_chunks);
}

PackedFilesWriter::FakeWriteBufferFromFile::FakeWriteBufferFromFile(const String & file_name_, std::shared_ptr<Data> data_)
    : WriteBufferFromFileBase(0, nullptr, 0)
    , file_name(file_name_)
    , data(std::move(data_))
{
    data->addChunk();
    set(data->chunks.back().data(), data->chunks.back().size());
}

void PackedFilesWriter::FakeWriteBufferFromFile::nextImpl()
{
    /// Flush before the current chunk is filled: continue writing into the rest of it.
    if (hasPendingData())
    {
        buffer() = Buffer(pos, buffer().end());
        return;
    }

    data->addChunk();
    set(data->chunks.back().data(), data->chunks.back().size());
}

void PackedFilesWriter::FakeWriteBufferFromFile::finalizeImpl()
{
    data->last_chunk_data_size = pos - data->chunks.back().data();
    data->size = count();
    data->is_finalized = true;
    data->state->non_spillable_bytes_in_memory -= data->chunks.back().size();

    /// The last chunk of the file can be moved out of memory only now. The files of a part are often
    /// finalized together right before the archive is written, so do not keep them in memory until then.
    if (auto * writer = data->state->writer)
        writer->spillIfNeeded();
}

}
