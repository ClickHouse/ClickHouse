#include <IO/Archives/createArchiveReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Storages/ObjectStorage/Iterators/ArchiveIterator.h>
#include <Storages/ObjectStorage/Iterators/IObjectIterator.h>
#include <Storages/ObjectStorage/Iterators/ObjectInfo.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Common/logger_useful.h>
#include <Common/parseGlobs.h>

namespace DB {


namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
}

static IArchiveReader::NameFilter createArchivePathFilter(const std::string & archive_pattern)
{
    auto matcher = std::make_shared<re2::RE2>(makeRegexpPatternFromGlobs(archive_pattern));
    if (!matcher->ok())
    {
        throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                        "Cannot compile regex from glob ({}): {}",
                        archive_pattern, matcher->error());
    }
    return [matcher](const std::string & p) mutable { return re2::RE2::FullMatch(p, *matcher); };
}

ArchiveIterator::ObjectInfoInArchive::ObjectInfoInArchive(
    ObjectInfoPtr archive_object_,
    const std::string & path_in_archive_,
    std::shared_ptr<IArchiveReader> archive_reader_,
    IArchiveReader::FileInfo && file_info_)
    : ObjectInfoBase(*archive_object_)
    , path_in_archive(path_in_archive_)
    , archive_reader(archive_reader_)
    , file_info(file_info_)
{
}

ArchiveIterator::ArchiveIterator(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    std::unique_ptr<IObjectIterator> archives_iterator_,
    ContextPtr context_,
    ObjectInfos * read_keys_,
    bool ignore_archive_globs_)
    : WithContext(context_)
    , object_storage(object_storage_)
    , is_path_in_archive_with_globs(configuration_->isPathInArchiveWithGlobs())
    , archives_iterator(std::move(archives_iterator_))
    , filter(is_path_in_archive_with_globs ? createArchivePathFilter(configuration_->getPathInArchive()) : IArchiveReader::NameFilter{})
    , log(getLogger("ArchiveIterator"))
    , path_in_archive(is_path_in_archive_with_globs ? "" : configuration_->getPathInArchive())
    , read_keys(read_keys_)
    , ignore_archive_globs(ignore_archive_globs_)
{
}

std::shared_ptr<IArchiveReader>
ArchiveIterator::createArchiveReader(ObjectInfoPtr object_info) const
{
    const auto size = object_info->base_object_info.metadata->size_bytes;
    return DB::createArchiveReader(
        /* path_to_archive */
        object_info->getPath(),
        /* archive_read_function */ [=, this]()
        { return createReadBuffer(object_info->base_object_info, object_storage, getContext(), log); },
        /* archive_size */ size);
}

ObjectInfoPtr ArchiveIterator::next(size_t processor)
{
    std::unique_lock lock{next_mutex};
    IArchiveReader::FileInfo current_file_info{};
    while (true)
    {
        if (filter)
        {
            if (!file_enumerator)
            {
                archive_object = archives_iterator->next(processor);
                if (!archive_object)
                {
                    LOG_TEST(log, "Archives are processed");
                    return {};
                }

                if (!archive_object->base_object_info.metadata)
                    archive_object->base_object_info.metadata = object_storage->getObjectMetadata(archive_object->getPath());

                archive_reader = createArchiveReader(archive_object);
                file_enumerator = archive_reader->firstFile();
                if (!file_enumerator)
                    continue;
            }
            else if (!file_enumerator->nextFile() || ignore_archive_globs)
            {
                file_enumerator.reset();
                continue;
            }

            path_in_archive = file_enumerator->getFileName();
            LOG_TEST(log, "Path in archive: {}", path_in_archive);
            if (!filter(path_in_archive))
                continue;
            current_file_info = file_enumerator->getFileInfo();
        }
        else
        {
            archive_object = archives_iterator->next(processor);
            if (!archive_object)
                return {};

            if (!archive_object->base_object_info.metadata)
                archive_object->base_object_info.metadata = object_storage->getObjectMetadata(archive_object->getPath());

            archive_reader = createArchiveReader(archive_object);
            if (!archive_reader->fileExists(path_in_archive))
                continue;
            current_file_info = archive_reader->getFileInfo(path_in_archive);
        }
        break;
    }

    auto object_in_archive
        = std::make_shared<ObjectInfoInArchive>(archive_object, path_in_archive, archive_reader, std::move(current_file_info));

    if (read_keys != nullptr)
        read_keys->push_back(object_in_archive);

    return object_in_archive;
}

size_t ArchiveIterator::estimatedKeysCount()
{
    return archives_iterator->estimatedKeysCount();
}

}
