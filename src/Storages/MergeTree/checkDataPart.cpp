#include <algorithm>
#include <optional>

#include <Poco/DirectoryIterator.h>

#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/HashingReadBuffer.h>
#include <Common/CurrentMetrics.h>


namespace CurrentMetrics
{
    extern const Metric ReplicatedChecks;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_PART_TYPE;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_MUNMAP;
    extern const int CANNOT_MREMAP;
    extern const int UNEXPECTED_FILE_IN_DATA_PART;
}


bool isNotEnoughMemoryErrorCode(int code)
{
    /// Don't count the part as broken if there is not enough memory to load it.
    /// In fact, there can be many similar situations.
    /// But it is OK, because there is a safety guard against deleting too many parts.
    return code == ErrorCodes::MEMORY_LIMIT_EXCEEDED
        || code == ErrorCodes::CANNOT_ALLOCATE_MEMORY
        || code == ErrorCodes::CANNOT_MUNMAP
        || code == ErrorCodes::CANNOT_MREMAP;
}


IMergeTreeDataPart::Checksums checkDataPart(
    MergeTreeData::DataPartPtr data_part,
    const DataPartStoragePtr & data_part_storage,
    const NamesAndTypesList & columns_list,
    const MergeTreeDataPartType & part_type,
    const NameSet & files_without_checksums,
    bool require_checksums,
    std::function<bool()> is_cancelled)
{
    /** Responsibility:
      * - read list of columns from columns.txt;
      * - read checksums if exist;
      * - validate list of columns and checksums
      */

    CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedChecks};

    NamesAndTypesList columns_txt;

    {
        auto buf = data_part_storage->readFile("columns.txt", {}, std::nullopt, std::nullopt);
        columns_txt.readText(*buf);
        assertEOF(*buf);
    }

    if (columns_txt != columns_list)
        throw Exception("Columns doesn't match in part " + data_part_storage->getFullPath()
            + ". Expected: " + columns_list.toString()
            + ". Found: " + columns_txt.toString(), ErrorCodes::CORRUPTED_DATA);

    /// Real checksums based on contents of data. Must correspond to checksums.txt. If not - it means the data is broken.
    IMergeTreeDataPart::Checksums checksums_data;

    /// This function calculates checksum for both compressed and decompressed contents of compressed file.
    auto checksum_compressed_file = [](const DataPartStoragePtr & data_part_storage_, const String & file_path)
    {
        auto file_buf = data_part_storage_->readFile(file_path, {}, std::nullopt, std::nullopt);
        HashingReadBuffer compressed_hashing_buf(*file_buf);
        CompressedReadBuffer uncompressing_buf(compressed_hashing_buf);
        HashingReadBuffer uncompressed_hashing_buf(uncompressing_buf);

        uncompressed_hashing_buf.ignoreAll();
        return IMergeTreeDataPart::Checksums::Checksum
        {
            compressed_hashing_buf.count(), compressed_hashing_buf.getHash(),
            uncompressed_hashing_buf.count(), uncompressed_hashing_buf.getHash()
        };
    };

    auto ratio_of_defaults = data_part->storage.getSettings()->ratio_of_defaults_for_sparse_serialization;
    SerializationInfoByName serialization_infos(columns_txt, SerializationInfo::Settings{ratio_of_defaults, false});

    if (data_part_storage->exists(IMergeTreeDataPart::SERIALIZATION_FILE_NAME))
    {
        auto serialization_file = data_part_storage->readFile(IMergeTreeDataPart::SERIALIZATION_FILE_NAME, {}, std::nullopt, std::nullopt);
        serialization_infos.readJSON(*serialization_file);
    }

    auto get_serialization = [&serialization_infos](const auto & column)
    {
        auto it = serialization_infos.find(column.name);
        return it == serialization_infos.end()
            ? column.type->getDefaultSerialization()
            : column.type->getSerialization(*it->second);
    };

    /// This function calculates only checksum of file content (compressed or uncompressed).
    /// It also calculates checksum of projections.
    auto checksum_file = [&](const String & file_name)
    {
        if (data_part_storage->isDirectory(file_name) && endsWith(file_name, ".proj"))
        {
            auto projection_name = file_name.substr(0, file_name.size() - sizeof(".proj") + 1);
            auto pit = data_part->getProjectionParts().find(projection_name);
            if (pit == data_part->getProjectionParts().end())
            {
                if (require_checksums)
                    throw Exception("Unexpected file " + file_name + " in data part", ErrorCodes::UNEXPECTED_FILE_IN_DATA_PART);
                else
                    return;
            }

            const auto & projection = pit->second;
            IMergeTreeDataPart::Checksums projection_checksums_data;

            auto projection_part_storage = data_part_storage->getProjection(file_name);

            if (projection->getType() == MergeTreeDataPartType::Compact)
            {
                auto file_buf = projection_part_storage->readFile(MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION, {}, std::nullopt, std::nullopt);
                HashingReadBuffer hashing_buf(*file_buf);
                hashing_buf.ignoreAll();
                projection_checksums_data.files[MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION]
                    = IMergeTreeDataPart::Checksums::Checksum(hashing_buf.count(), hashing_buf.getHash());
            }
            else
            {
                const NamesAndTypesList & projection_columns_list = projection->getColumns();
                for (const auto & projection_column : projection_columns_list)
                {
                    get_serialization(projection_column)->enumerateStreams(
                        [&](const ISerialization::SubstreamPath & substream_path)
                        {
                            String projection_file_name = ISerialization::getFileNameForStream(projection_column, substream_path) + ".bin";
                            projection_checksums_data.files[projection_file_name] = checksum_compressed_file(projection_part_storage, projection_file_name);
                        });
                }
            }

            IMergeTreeDataPart::Checksums projection_checksums_txt;

            if (require_checksums || projection_part_storage->exists("checksums.txt"))
            {
                auto buf = projection_part_storage->readFile("checksums.txt", {}, std::nullopt, std::nullopt);
                projection_checksums_txt.read(*buf);
                assertEOF(*buf);
            }

            const auto & projection_checksum_files_txt = projection_checksums_txt.files;
            for (auto projection_it = projection_part_storage->iterate(); projection_it->isValid(); projection_it->next())
            {
                const String & projection_file_name = projection_it->name();
                auto projection_checksum_it = projection_checksums_data.files.find(projection_file_name);

                /// Skip files that we already calculated. Also skip metadata files that are not checksummed.
                if (projection_checksum_it == projection_checksums_data.files.end() && !files_without_checksums.contains(projection_file_name))
                {
                    auto projection_txt_checksum_it = projection_checksum_files_txt.find(file_name);
                    if (projection_txt_checksum_it == projection_checksum_files_txt.end()
                        || projection_txt_checksum_it->second.uncompressed_size == 0)
                    {
                        auto projection_file_buf = projection_part_storage->readFile(projection_file_name, {}, std::nullopt, std::nullopt);
                        HashingReadBuffer projection_hashing_buf(*projection_file_buf);
                        projection_hashing_buf.ignoreAll();
                        projection_checksums_data.files[projection_file_name] = IMergeTreeDataPart::Checksums::Checksum(
                            projection_hashing_buf.count(), projection_hashing_buf.getHash());
                    }
                    else
                    {
                        projection_checksums_data.files[projection_file_name] = checksum_compressed_file(projection_part_storage, projection_file_name);
                    }
                }
            }
            checksums_data.files[file_name] = IMergeTreeDataPart::Checksums::Checksum(
                projection_checksums_data.getTotalSizeOnDisk(), projection_checksums_data.getTotalChecksumUInt128());

            if (require_checksums || !projection_checksums_txt.files.empty())
                projection_checksums_txt.checkEqual(projection_checksums_data, false);
        }
        else
        {
            auto file_buf = data_part_storage->readFile(file_name, {}, std::nullopt, std::nullopt);
            HashingReadBuffer hashing_buf(*file_buf);
            hashing_buf.ignoreAll();
            checksums_data.files[file_name] = IMergeTreeDataPart::Checksums::Checksum(hashing_buf.count(), hashing_buf.getHash());
        }
    };

    bool check_uncompressed = true;
    /// First calculate checksums for columns data
    if (part_type == MergeTreeDataPartType::Compact)
    {
        checksum_file(MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION);
        /// Uncompressed checksums in compact parts are computed in a complex way.
        /// We check only checksum of compressed file.
        check_uncompressed = false;
    }
    else if (part_type == MergeTreeDataPartType::Wide)
    {
        for (const auto & column : columns_list)
        {
            get_serialization(column)->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
            {
                String file_name = ISerialization::getFileNameForStream(column, substream_path) + ".bin";
                checksums_data.files[file_name] = checksum_compressed_file(data_part_storage, file_name);
            });
        }
    }
    else
    {
        throw Exception("Unknown type in part " + data_part_storage->getFullPath(), ErrorCodes::UNKNOWN_PART_TYPE);
    }

    /// Checksums from the rest files listed in checksums.txt. May be absent. If present, they are subsequently compared with the actual data checksums.
    IMergeTreeDataPart::Checksums checksums_txt;

    if (require_checksums || data_part_storage->exists("checksums.txt"))
    {
        auto buf = data_part_storage->readFile("checksums.txt", {}, std::nullopt, std::nullopt);
        checksums_txt.read(*buf);
        assertEOF(*buf);
    }

    const auto & checksum_files_txt = checksums_txt.files;
    for (auto it = data_part_storage->iterate(); it->isValid(); it->next())
    {
        const String & file_name = it->name();
        auto checksum_it = checksums_data.files.find(file_name);

        /// Skip files that we already calculated. Also skip metadata files that are not checksummed.
        if (checksum_it == checksums_data.files.end() && !files_without_checksums.contains(file_name))
        {
            auto txt_checksum_it = checksum_files_txt.find(file_name);
            if (txt_checksum_it == checksum_files_txt.end() || txt_checksum_it->second.uncompressed_size == 0)
            {
                /// The file is not compressed.
                checksum_file(file_name);
            }
            else /// If we have both compressed and uncompressed in txt, then calculate them
            {
                checksums_data.files[file_name] = checksum_compressed_file(data_part_storage, file_name);
            }
        }
    }

    if (is_cancelled())
        return {};

    if (require_checksums || !checksums_txt.files.empty())
        checksums_txt.checkEqual(checksums_data, check_uncompressed);
    return checksums_data;
}

IMergeTreeDataPart::Checksums checkDataPartInMemory(const DataPartInMemoryPtr & data_part)
{
    IMergeTreeDataPart::Checksums data_checksums;
    data_checksums.files["data.bin"] = data_part->calculateBlockChecksum();
    data_part->checksums.checkEqual(data_checksums, true);
    return data_checksums;
}

IMergeTreeDataPart::Checksums checkDataPart(
    MergeTreeData::DataPartPtr data_part,
    bool require_checksums,
    std::function<bool()> is_cancelled)
{
    if (auto part_in_memory = asInMemoryPart(data_part))
        return checkDataPartInMemory(part_in_memory);

    return checkDataPart(
        data_part,
        data_part->data_part_storage,
        data_part->getColumns(),
        data_part->getType(),
        data_part->getFileNamesWithoutChecksums(),
        require_checksums,
        is_cancelled);
}

}
