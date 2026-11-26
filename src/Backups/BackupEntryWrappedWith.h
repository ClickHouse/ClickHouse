#pragma once

#include <Backups/IBackupEntry.h>
#include <IO/SeekableReadBuffer.h>

namespace DB
{

/// Wraps another backup entry and a value of any type.
template <typename T>
class BackupEntryWrappedWith : public IBackupEntry
{
public:
    BackupEntryWrappedWith(BackupEntryPtr entry_, const T & custom_value_) : entry(entry_), custom_value(custom_value_) { }
    BackupEntryWrappedWith(BackupEntryPtr entry_, T && custom_value_) : entry(entry_), custom_value(std::move(custom_value_)) { }
    ~BackupEntryWrappedWith() override = default;

    [[nodiscard]] std::unique_ptr<SeekableReadBuffer> getReadBuffer(const ReadSettings & read_settings) const override { return entry->getReadBuffer(read_settings); }
    [[nodiscard]] std::unique_ptr<SeekableReadBuffer> getReadBufferForEncryptionHeader(const ReadSettings & read_settings) const override
    {
        return entry->getReadBufferForEncryptionHeader(read_settings);
    }
    [[nodiscard]] UInt64 getSize() const override { return entry->getSize(); }
    [[nodiscard]] UInt128 getChecksum(const ReadSettings & read_settings) const override { return entry->getChecksum(read_settings); }
    [[nodiscard]] std::optional<UInt128> getPartialChecksum(UInt64 limit, const ReadSettings & read_settings) const override { return entry->getPartialChecksum(limit, read_settings); }
    [[nodiscard]] DataSourceDescription getDataSourceDescription() const override { return entry->getDataSourceDescription(); }
    [[nodiscard]] bool isEncryptedByDisk() const override { return entry->isEncryptedByDisk(); }
    [[nodiscard]] bool isFromFile() const override { return entry->isFromFile(); }
    [[nodiscard]] bool isFromImmutableFile() const override { return entry->isFromImmutableFile(); }
    [[nodiscard]] bool isFromRemoteFile() const override { return entry->isFromRemoteFile(); }
    [[nodiscard]] bool isFromSnapshot() const override { return entry->isFromSnapshot(); }
    [[nodiscard]] bool isNativeCopyAllowed() const override { return entry->isNativeCopyAllowed(); }
    [[nodiscard]] String getEndpointURI() const override { return entry->getEndpointURI(); }
    [[nodiscard]] String getNamespace() const override { return entry->getNamespace(); }
    [[nodiscard]] String getRemotePath() const override { return entry->getRemotePath(); }
    [[nodiscard]] String getFilePath() const override { return entry->getFilePath(); }
    [[nodiscard]] DiskPtr getDisk() const override { return entry->getDisk(); }

private:
    BackupEntryPtr entry;
    T custom_value;
};

template <typename T>
BackupEntryPtr wrapBackupEntryWith(BackupEntryPtr && backup_entry, const T & custom_value)
{
    return std::make_shared<BackupEntryWrappedWith<T>>(std::move(backup_entry), custom_value);
}

template <typename T>
void wrapBackupEntriesWith(std::vector<std::pair<String, BackupEntryPtr>> & backup_entries, const T & custom_value)
{
    for (auto & [_, backup_entry] : backup_entries)
        backup_entry = wrapBackupEntryWith(std::move(backup_entry), custom_value);
}

}
