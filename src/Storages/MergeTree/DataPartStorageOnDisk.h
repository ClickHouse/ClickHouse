#pragma once
#include <Storages/MergeTree/IDataPartStorage.h>
#include <memory>
#include <string>

namespace DB
{

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;


class DataPartStorageOnDisk final : public IDataPartStorage
{
public:
    explicit DataPartStorageOnDisk(VolumePtr volume_, std::string root_path_);

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    bool exists(const std::string & path) const override;
    bool exists() const override;

    size_t getFileSize(const std::string & path) const override;

    DiskDirectoryIteratorPtr iterate() const override;
    DiskDirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    std::string getFullPath() const override;
    std::string getFullRelativePath() const override { return root_path; }

    UInt64 calculateTotalSizeOnDisk() const override;

    void writeChecksums(MergeTreeDataPartChecksums & checksums) const override;
    void writeColumns(NamesAndTypesList & columns) const override;

    bool shallParticipateInMerges(const IStoragePolicy &) const;

    void rename(const String & new_relative_path, Poco::Logger * log, bool remove_new_dir_if_exists, bool fsync) override;

    std::string getName() const override;

    DataPartStoragePtr getProjection(const std::string & name) const override;

private:
    VolumePtr volume;
    std::string root_path;
};

}
