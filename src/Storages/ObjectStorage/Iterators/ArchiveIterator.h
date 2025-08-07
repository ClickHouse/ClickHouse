#include <Common/re2.h>
#include <Interpreters/Context_fwd.h>
#include <IO/Archives/IArchiveReader.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/Iterators/IObjectIterator.h>


namespace DB {
/*
 * An archives iterator.
 * Allows to iterate files inside one or many archives.
 * `archives_iterator` is an iterator which iterates over different archives.
 * There are two ways to read files in archives:
 * 1. When we want to read one concete file in each archive.
 *    In this case we go through all archives, check if this certain file
 *    exists within this archive and read it if it exists.
 * 2. When we have a certain pattern of files we want to read in each archive.
 *    For this purpose we create a filter defined as IArchiveReader::NameFilter.
 */
class ArchiveIterator : public IObjectIterator, private WithContext
{
public:
    explicit ArchiveIterator(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        std::unique_ptr<IObjectIterator> archives_iterator_,
        ContextPtr context_,
        ObjectInfos * read_keys_,
        bool ignore_archive_globs_ = false);

    ObjectInfoPtr next(size_t processor) override;

    size_t estimatedKeysCount() override;

    struct ObjectInfoInArchive : public ObjectInfo
    {
        ObjectInfoInArchive(
            ObjectInfoPtr archive_object_,
            const std::string & path_in_archive_,
            std::shared_ptr<IArchiveReader> archive_reader_,
            IArchiveReader::FileInfo && file_info_);

        std::string getFileName() const override
        {
            return path_in_archive;
        }

        std::string getPath() const override
        {
            return archive_object->getPath() + "::" + path_in_archive;
        }

        std::string getPathToArchive() const override
        {
            return archive_object->getPath();
        }

        bool isArchive() const override { return true; }

        size_t fileSizeInArchive() const override { return file_info.uncompressed_size; }

        const ObjectInfoPtr archive_object;
        const std::string path_in_archive;
        const std::shared_ptr<IArchiveReader> archive_reader;
        const IArchiveReader::FileInfo file_info;
    };

private:
    std::shared_ptr<IArchiveReader> createArchiveReader(ObjectInfoPtr object_info) const;

    const ObjectStoragePtr object_storage;
    const bool is_path_in_archive_with_globs;
    /// Iterator which iterates through different archives.
    const std::unique_ptr<IObjectIterator> archives_iterator;
    /// Used when files inside archive are defined with a glob
    const IArchiveReader::NameFilter filter = {};
    const LoggerPtr log;
    /// Current file inside the archive.
    std::string path_in_archive = {};
    /// Read keys of files inside archives.
    ObjectInfos * read_keys;
    /// Object pointing to archive (NOT path within archive).
    ObjectInfoPtr archive_object;
    /// Reader of the archive.
    std::shared_ptr<IArchiveReader> archive_reader;
    /// File enumerator inside the archive.
    std::unique_ptr<IArchiveReader::FileEnumerator> file_enumerator;

    bool ignore_archive_globs;

    std::mutex next_mutex;
};
}
