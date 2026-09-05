#include <IO/WriteSettings.h>
#include <IO/WriteHelpers.h>
#include <IO/PackedFilesWriter.h>
#include <IO/WriteBufferFromString.h>
#include <Common/escapeForFileName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
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
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "File {} already exists", file_name);

    it->second = std::make_shared<Data>();
    return std::make_unique<FakeWriteBufferFromFile>(file_name, it->second);
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


void PackedFilesWriter::writePackedIndex(WriteBuffer & out, const PackedFilesIO::Index & index)
{
    writeIntBinary(PackedFilesIO::VERSION, out);
    writeIntBinary(index.size(), out);

    for (const auto & [name, offset] : index)
    {
        writeStringBinary(name, out);
        writeIntBinary(offset.offset, out);
        writeIntBinary(offset.size, out);
    }
}

PackedFilesIO::Index PackedFilesWriter::finalize(CommitDataFunc commit_func, const Strings & files_order_hint)
{
    WriteBufferFromOwnString serialized;
    auto [index, need_sync] = finalize(serialized, files_order_hint);

    serialized.preFinalize();
    serialized.finalize();
    commit_func(serialized.str(), write_settings.value_or(WriteSettings{}), need_sync);
    return index;
}

std::pair<PackedFilesIO::Index, bool> PackedFilesWriter::finalize(WriteBuffer & out, const Strings & files_order_hint)
{
    for (auto & change : metadata_changes)
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
    writeIntBinary(PackedFilesIO::VERSION, out);
    writeIntBinary(num_files, out);

    std::vector<String> ordered_file_names;
    ordered_file_names.reserve(num_files);
    /// Order files according to the hint.
    {
        std::unordered_set<String> already_added_files;
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
    UInt64 data_offset = getSizeOfHeader();
    for (const auto & name : ordered_file_names)
    {
        /// 3 fields: file_name, offset, size.
        data_offset += getLengthOfSerializedString(name) + sizeof(UInt64) * 2;
    }

    PackedFilesIO::Index index;
    for (const auto & name : ordered_file_names)
    {
        const auto & data = written_files[name];
        const UInt64 data_size = data->chars.size();
        writeStringBinary(name, out);
        writeIntBinary(data_offset, out);
        writeIntBinary(data_size, out);

        index[name] = {data_offset, data_size};
        data_offset += data_size;
    }

    /// fsync the whole file with archive if any of files were requested to be fsynced.
    bool need_sync = false;
    for (const auto & name : ordered_file_names)
    {
        const auto & data = written_files[name];
        out.write(reinterpret_cast<const char *>(data->chars.data()), data->chars.size());
        need_sync |= data->need_sync;
    }

    return {index, need_sync};
}

size_t PackedFilesWriter::getSizeOfHeader()
{
    /// 2 fields: version, number of files.
    return sizeof(PackedFilesIO::VERSION) + sizeof(UInt64);
}

void PackedFilesWriter::FakeWriteBufferFromFile::nextImpl()
{
    SwapGuard guard(*this);
    impl->next();
}

void PackedFilesWriter::FakeWriteBufferFromFile::finalizeImpl()
{
    SwapGuard guard(*this);
    impl->finalize();
}

void PackedFilesWriter::FakeWriteBufferFromFile::cancelImpl() noexcept
{
    SwapGuard guard(*this);
    impl->cancel();
}

}
