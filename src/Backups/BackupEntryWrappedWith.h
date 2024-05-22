#pragma once

#include <Backups/IBackupEntry.h>


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

    std::unique_ptr<SeekableReadBuffer> getReadBuffer(const ReadSettings & read_settings) const override { return entry->getReadBuffer(read_settings); }
    UInt64 getSize() const override { return entry->getSize(); }
    UInt128 getChecksum(const ReadSettings & read_settings) const override { return entry->getChecksum(read_settings); }
    std::optional<UInt128> getPartialChecksum(size_t prefix_length, const ReadSettings & read_settings) const override { return entry->getPartialChecksum(prefix_length, read_settings); }
    DataSourceDescription getDataSourceDescription() const override { return entry->getDataSourceDescription(); }
    bool isEncryptedByDisk() const override { return entry->isEncryptedByDisk(); }
    bool isFromFile() const override { return entry->isFromFile(); }
    bool isFromImmutableFile() const override { return entry->isFromImmutableFile(); }
    String getFilePath() const override { return entry->getFilePath(); }
    DiskPtr getDisk() const override { return entry->getDisk(); }

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
