#include <Storages/MergeTree/PrimaryIndexLoader.h>

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadSettings.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event LoadedPrimaryIndexFiles;
    extern const Event LoadedPrimaryIndexRows;
    extern const Event LoadedPrimaryIndexBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int EXPECTED_END_OF_FILE;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsFloat primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns;
}

String getPrimaryIndexFileName(const IDataPartStorage & data_part_storage)
{
    if (data_part_storage.existsFile("primary" + getIndexExtension(true)))
        return "primary" + getIndexExtension(true);
    return "primary" + getIndexExtension(false);
}

template <typename Columns>
void dropPrimaryIndexSuffixColumns(size_t marks_count, Columns & index_columns, Float64 ratio_to_drop_suffix_columns)
{
    const size_t key_size = index_columns.size();
    if (marks_count == 0 || key_size <= 1 || ratio_to_drop_suffix_columns <= 0 || ratio_to_drop_suffix_columns >= 1)
        return;

    for (size_t j = 0; j < key_size - 1; ++j)
    {
        size_t num_changes = 0;
        for (size_t i = 1; i < marks_count; ++i)
        {
            if (0 != index_columns[j]->compareAt(i, i - 1, *index_columns[j], 0))
                ++num_changes;
        }

        if (static_cast<Float64>(num_changes) / static_cast<Float64>(marks_count) >= ratio_to_drop_suffix_columns)
        {
            index_columns.resize(j + 1);
            break;
        }
    }
}

template void dropPrimaryIndexSuffixColumns<Columns>(size_t, Columns &, Float64);
template void dropPrimaryIndexSuffixColumns<MutableColumns>(size_t, MutableColumns &, Float64);

Columns loadPrimaryIndexFromStorage(
    const IDataPartStorage & data_part_storage,
    const KeyDescription & primary_key,
    size_t marks_count,
    const MergeTreeSettings & storage_settings,
    const ReadSettings & read_settings_,
    bool keep_all_columns)
{
    const size_t key_size = primary_key.column_names.size();
    if (!key_size)
        return {};

    MutableColumns loaded_index;
    loaded_index.resize(key_size);

    for (size_t i = 0; i < key_size; ++i)
    {
        loaded_index[i] = primary_key.data_types[i]->createColumn();
        loaded_index[i]->reserve(marks_count);
    }

    const String index_name = getPrimaryIndexFileName(data_part_storage);
    const String index_path = fs::path(data_part_storage.getRelativePath()) / index_name;

    /// Opened like `IMergeTreeDataPart::readFile` opens the small metadata files of a part.
    constexpr size_t size_hint = 4096;
    auto read_settings = read_settings_.adjustBufferSize(size_hint);
    read_settings.local_fs_settings.method = LocalFSReadMethod::pread;
    if (isPackedPartStorage(data_part_storage)
        && !read_settings.filesystem_cache_settings.allow_background_download_for_metadata_files_in_packed_storage)
        read_settings.filesystem_cache_settings.allow_background_download = false;

    auto raw_file = data_part_storage.readFile(index_name, read_settings, size_hint);
    std::unique_ptr<ReadBuffer> index_file;
    if (isCompressedFromIndexExtension(fs::path(index_name).extension()))
        index_file = std::make_unique<CompressedReadBufferFromFile>(std::move(raw_file));
    else
        index_file = std::move(raw_file);

    Serializations key_serializations(key_size);
    for (size_t j = 0; j < key_size; ++j)
        key_serializations[j] = primary_key.data_types[j]->getDefaultSerialization();

    FormatSettings format_settings;
    for (size_t i = 0; i < marks_count; ++i)
    {
        for (size_t j = 0; j < key_size; ++j)
            key_serializations[j]->deserializeBinary(*loaded_index[j], *index_file, format_settings);
    }

    if (!keep_all_columns)
        dropPrimaryIndexSuffixColumns(
            marks_count,
            loaded_index,
            static_cast<Float64>(storage_settings[MergeTreeSetting::primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns]));

    size_t total_bytes = 0;
    for (const auto & column : loaded_index)
    {
        column->shrinkToFit();
        column->protect();
        total_bytes += column->byteSize();

        if (column->size() != marks_count)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all data from index file {}(expected size: "
                "{}, read: {})", index_path, marks_count, column->size());
    }

    if (!index_file->eof())
        throw Exception(ErrorCodes::EXPECTED_END_OF_FILE, "Index file {} is unexpectedly long", index_path);

    ProfileEvents::increment(ProfileEvents::LoadedPrimaryIndexFiles);
    ProfileEvents::increment(ProfileEvents::LoadedPrimaryIndexRows, marks_count);
    ProfileEvents::increment(ProfileEvents::LoadedPrimaryIndexBytes, total_bytes);

    return Columns(std::make_move_iterator(loaded_index.begin()), std::make_move_iterator(loaded_index.end()));
}

}
