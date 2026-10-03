#include <Common/UnorderedSetWithMemoryTracking.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <IO/WriteHelpers.h>
#include <IO/PackedFilesWriter.h>
#include <IO/SwapHelper.h>
#include <IO/copyData.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/escapeForFileName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int FILE_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
}

PackedFilesWriter::PackedFilesWriter(std::shared_ptr<SpillConfig> spill_config_)
    : spill_config(std::move(spill_config_))
{
}

PackedFilesWriter::~PackedFilesWriter()
{
    try
    {
        if (spill_config && spill_config->remove_spill_temp_dir)
            spill_config->remove_spill_temp_dir();
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("PackedFilesWriter"), "Failed to remove spill temp directory");
    }
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

    auto file = std::make_shared<WrittenFile>(std::make_shared<SpillableMemoryWriteBuffer>(spill_config, file_name));
    it->second = file;
    return std::make_unique<FakeWriteBufferFromFile>(file);
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
        const auto & file = written_files.at(name);
        const UInt64 data_size = file->buffer->count();

        index[name] = {data_offset, data_size, file->uncompressed_size};
        data_offset += data_size;
        /// fsync the whole file with archive if any of files were requested to be fsynced.
        need_sync |= file->need_sync;
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

    for (const auto & name : plan.ordered_file_names)
    {
        const auto & file = written_files.at(name);
        auto read_buf = file->buffer->tryGetReadBuffer();
        if (!read_buf)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Failed to open read buffer for file {} from packed archive", name);
        copyData(*read_buf, out);
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

PackedFilesWriter::FakeWriteBufferFromFile::FakeWriteBufferFromFile(std::shared_ptr<WrittenFile> file_)
    : WriteBufferFromFileBase(0, nullptr, 0)
    , file(std::move(file_))
{
    swap(*file->buffer);
}

PackedFilesWriter::FakeWriteBufferFromFile::~FakeWriteBufferFromFile()
{
    swap(*file->buffer);
}

void PackedFilesWriter::FakeWriteBufferFromFile::nextImpl()
{
    SwapHelper swap_helper(*this, *file->buffer);
    file->buffer->next();
}

void PackedFilesWriter::FakeWriteBufferFromFile::finalizeImpl()
{
    SwapHelper swap_helper(*this, *file->buffer);
    file->buffer->finalize();
}

void PackedFilesWriter::FakeWriteBufferFromFile::cancelImpl() noexcept
{
    SwapHelper swap_helper(*this, *file->buffer);
    file->buffer->cancel();
}

}
