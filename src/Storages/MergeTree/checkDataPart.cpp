#include <Poco/Logger.h>
#include <optional>

#include <Poco/DirectoryIterator.h>

#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/HashingReadBuffer.h>
#include <IO/S3Common.h>
#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <IO/AzureBlobStorage/isRetryableAzureException.h>
#include <Poco/Net/NetException.h>


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
    extern const int CANNOT_SCHEDULE_TASK;
    extern const int UNEXPECTED_FILE_IN_DATA_PART;
    extern const int NO_FILE_IN_DATA_PART;
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int BROKEN_PROJECTION;
    extern const int ABORTED;
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

bool isRetryableException(std::exception_ptr exception_ptr)
{
    try
    {
        rethrow_exception(exception_ptr);
    }
#if USE_AWS_S3
    catch (const S3Exception & s3_exception)
    {
        return s3_exception.isRetryableError();
    }
#endif
#if USE_AZURE_BLOB_STORAGE
    catch (const Azure::Core::RequestFailedException & e)
    {
        return isRetryableAzureException(e);
    }
#endif
    catch (const ErrnoException & e)
    {
        return e.getErrno() == EMFILE;
    }
    catch (const Coordination::Exception & e)
    {
        return Coordination::isHardwareError(e.code);
    }
    catch (const Exception & e)
    {
        return isNotEnoughMemoryErrorCode(e.code())
            || e.code() == ErrorCodes::NETWORK_ERROR
            || e.code() == ErrorCodes::SOCKET_TIMEOUT
            || e.code() == ErrorCodes::CANNOT_SCHEDULE_TASK
            || e.code() == ErrorCodes::ABORTED;
    }
    catch (const Poco::Net::NetException &)
    {
        return true;
    }
    catch (const Poco::TimeoutException &)
    {
        return true;
    }
    catch (...)
    {
        /// In fact, there can be other similar situations.
        /// But it is OK, because there is a safety guard against deleting too many parts.
        return false;
    }
}


static IMergeTreeDataPart::Checksums checkDataPart(
    MergeTreeData::DataPartPtr data_part,
    const IDataPartStorage & data_part_storage,
    const NamesAndTypesList & columns_list,
    const MergeTreeDataPartType & part_type,
    const NameSet & files_without_checksums,
    const ReadSettings & read_settings,
    bool require_checksums,
    std::function<bool()> is_cancelled,
    bool & is_broken_projection,
    bool throw_on_broken_projection)
{
    /** Responsibility:
      * - read list of columns from columns.txt;
      * - read checksums if exist;
      * - validate list of columns and checksums
      */

    CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedChecks};
    Poco::Logger * log = &Poco::Logger::get("checkDataPart");

    NamesAndTypesList columns_txt;

    {
        auto buf = data_part_storage.readFile("columns.txt", read_settings, std::nullopt, std::nullopt);
        columns_txt.readText(*buf);
        assertEOF(*buf);
    }

    if (columns_txt != columns_list)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Columns doesn't match in part {}. Expected: {}. Found: {}",
            data_part_storage.getFullPath(), columns_list.toString(), columns_txt.toString());

    /// Real checksums based on contents of data. Must correspond to checksums.txt. If not - it means the data is broken.
    IMergeTreeDataPart::Checksums checksums_data;

    /// This function calculates checksum for both compressed and decompressed contents of compressed file.
    auto checksum_compressed_file = [&read_settings](const IDataPartStorage & data_part_storage_, const String & file_path)
    {
        auto file_buf = data_part_storage_.readFile(file_path, read_settings, std::nullopt, std::nullopt);
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
    SerializationInfoByName serialization_infos;

    if (data_part_storage.exists(IMergeTreeDataPart::SERIALIZATION_FILE_NAME))
    {
        try
        {
            auto serialization_file = data_part_storage.readFile(IMergeTreeDataPart::SERIALIZATION_FILE_NAME, read_settings, std::nullopt, std::nullopt);
            SerializationInfo::Settings settings{ratio_of_defaults, false};
            serialization_infos = SerializationInfoByName::readJSON(columns_txt, settings, *serialization_file);
        }
        catch (const Poco::Exception & ex)
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Failed to load {}, with error {}", IMergeTreeDataPart::SERIALIZATION_FILE_NAME, ex.message());
        }
        catch (...)
        {
            throw;
        }
    }

    auto get_serialization = [&serialization_infos](const auto & column)
    {
        auto it = serialization_infos.find(column.name);
        return it == serialization_infos.end()
            ? column.type->getDefaultSerialization()
            : column.type->getSerialization(*it->second);
    };

    /// This function calculates only checksum of file content (compressed or uncompressed).
    auto checksum_file = [&](const String & file_name)
    {
        auto file_buf = data_part_storage.readFile(file_name, read_settings, std::nullopt, std::nullopt);
        HashingReadBuffer hashing_buf(*file_buf);
        hashing_buf.ignoreAll();
        checksums_data.files[file_name] = IMergeTreeDataPart::Checksums::Checksum(hashing_buf.count(), hashing_buf.getHash());
    };

    /// Do not check uncompressed for projections. But why?
    bool check_uncompressed = !data_part->isProjectionPart();

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
                /// Skip ephemeral subcolumns that don't store any real data.
                if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                    return;

                auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(column, substream_path, ".bin", data_part_storage);

                if (!stream_name)
                    throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART,
                        "There is no file for column '{}' in data part '{}'",
                        column.name, data_part->name);

                auto file_name = *stream_name + ".bin";
                checksums_data.files[file_name] = checksum_compressed_file(data_part_storage, file_name);
            }, column.type, data_part->getColumnSample(column));
        }
    }
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_PART_TYPE, "Unknown type in part {}", data_part_storage.getFullPath());
    }

    /// Checksums from the rest files listed in checksums.txt. May be absent. If present, they are subsequently compared with the actual data checksums.
    IMergeTreeDataPart::Checksums checksums_txt;

    if (require_checksums || data_part_storage.exists("checksums.txt"))
    {
        auto buf = data_part_storage.readFile("checksums.txt", read_settings, std::nullopt, std::nullopt);
        checksums_txt.read(*buf);
        assertEOF(*buf);
    }

    NameSet projections_on_disk;
    const auto & checksums_txt_files = checksums_txt.files;
    for (auto it = data_part_storage.iterate(); it->isValid(); it->next())
    {
        auto file_name = it->name();

        /// We will check projections later.
        if (data_part_storage.isDirectory(file_name) && file_name.ends_with(".proj"))
        {
            projections_on_disk.insert(file_name);
            continue;
        }

        /// Exclude files written by full-text index from check. No correct checksums are available for them currently.
        if (isGinFile(file_name))
            continue;

        auto checksum_it = checksums_data.files.find(file_name);
        /// Skip files that we already calculated. Also skip metadata files that are not checksummed.
        if (checksum_it == checksums_data.files.end() && !files_without_checksums.contains(file_name))
        {
            auto txt_checksum_it = checksums_txt_files.find(file_name);
            if ((txt_checksum_it != checksums_txt_files.end() && txt_checksum_it->second.is_compressed))
            {
                /// If we have both compressed and uncompressed in txt or its .cmrk(2/3) or .cidx, then calculate them
                checksums_data.files[file_name] = checksum_compressed_file(data_part_storage, file_name);
            }
            else
            {
                /// The file is not compressed.
                checksum_file(file_name);
            }
        }
    }

    std::string broken_projections_message;
    for (const auto & [name, projection] : data_part->getProjectionParts())
    {
        if (is_cancelled())
            return {};

        auto projection_file = name + ".proj";

        IMergeTreeDataPart::Checksums projection_checksums;
        try
        {
            bool noop;
            projection_checksums = checkDataPart(
                projection, *data_part_storage.getProjection(projection_file),
                projection->getColumns(), projection->getType(),
                projection->getFileNamesWithoutChecksums(),
                read_settings, require_checksums, is_cancelled, noop, /* throw_on_broken_projection */false);
        }
        catch (...)
        {
            if (isRetryableException(std::current_exception()))
                throw;

            is_broken_projection = true;
            projections_on_disk.erase(projection_file);
            checksums_txt.remove(projection_file);

            const auto exception_message = getCurrentExceptionMessage(true);

            if (!projection->is_broken)
            {
                LOG_WARNING(log, "Marking projection {} as broken ({}). Reason: {}",
                            name, projection_file, exception_message);
                projection->setBrokenReason(exception_message, getCurrentExceptionCode());
            }

            if (throw_on_broken_projection)
            {
                if (!broken_projections_message.empty())
                    broken_projections_message += "\n";

                broken_projections_message += fmt::format(
                    "Part `{}` has broken projection `{}` (error: {})",
                    data_part->name, name, exception_message);
            }

            continue;
        }

        checksums_data.files[projection_file] = IMergeTreeDataPart::Checksums::Checksum(
            projection_checksums.getTotalSizeOnDisk(),
            projection_checksums.getTotalChecksumUInt128());

        projections_on_disk.erase(projection_file);
    }

    if (throw_on_broken_projection)
    {
        if (!broken_projections_message.empty())
        {
            throw Exception(ErrorCodes::BROKEN_PROJECTION, "{}", broken_projections_message);
        }

        /// This one is actually not broken, just redundant files on disk which
        /// MergeTree will never use.
        if (require_checksums && !projections_on_disk.empty())
        {
            throw Exception(ErrorCodes::UNEXPECTED_FILE_IN_DATA_PART,
                "Found unexpected projection directories: {}",
                fmt::join(projections_on_disk, ","));
        }
    }

    if (is_cancelled())
        return {};

    if (require_checksums || !checksums_txt.files.empty())
        checksums_txt.checkEqual(checksums_data, check_uncompressed, data_part->name);

    return checksums_data;
}

IMergeTreeDataPart::Checksums checkDataPart(
    MergeTreeData::DataPartPtr data_part,
    bool require_checksums,
    bool & is_broken_projection,
    std::function<bool()> is_cancelled,
    bool throw_on_broken_projection)
{
    /// If check of part has failed and it is stored on disk with cache
    /// try to drop cache and check it once again because maybe the cache
    /// is broken not the part itself.
    auto drop_cache_and_check = [&]
    {
        const auto & data_part_storage = data_part->getDataPartStorage();
        auto cache_name = data_part_storage.getCacheName();

        if (!cache_name)
            throw;

        LOG_DEBUG(
            getLogger("checkDataPart"),
            "Will drop cache for data part {} and will check it once again", data_part->name);

        auto & cache = *FileCacheFactory::instance().getByName(*cache_name)->cache;
        for (auto it = data_part_storage.iterate(); it->isValid(); it->next())
        {
            auto file_name = it->name();
            if (!data_part_storage.isDirectory(file_name))
            {
                auto remote_paths = data_part_storage.getRemotePaths(file_name);
                for (const auto & remote_path : remote_paths)
                    cache.removePathIfExists(remote_path, FileCache::getCommonUser().user_id);
            }
        }

        ReadSettings read_settings;
        read_settings.enable_filesystem_cache = false;

        return checkDataPart(
            data_part,
            data_part_storage,
            data_part->getColumns(),
            data_part->getType(),
            data_part->getFileNamesWithoutChecksums(),
            read_settings,
            require_checksums,
            is_cancelled,
            is_broken_projection,
            throw_on_broken_projection);
    };

    try
    {
        ReadSettings read_settings;
        return checkDataPart(
            data_part,
            data_part->getDataPartStorage(),
            data_part->getColumns(),
            data_part->getType(),
            data_part->getFileNamesWithoutChecksums(),
            read_settings,
            require_checksums,
            is_cancelled,
            is_broken_projection,
            throw_on_broken_projection);
    }
    catch (...)
    {
        if (isRetryableException(std::current_exception()))
            throw;
        return drop_cache_and_check();
    }
}

}
