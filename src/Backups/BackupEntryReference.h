#pragma once

#include <Backups/IBackupEntry.h>

namespace DB
{

/// Represents a reference to another backup entry.
class BackupEntryReference : public IBackupEntry
{
public:
    explicit BackupEntryReference(std::string reference_target_);

    UInt64 getSize() const override;
    [[noreturn]] UInt128 getChecksum(const ReadSettings & read_settings) const override;
    [[noreturn]] std::unique_ptr<SeekableReadBuffer> getReadBuffer(const ReadSettings & read_settings) const override;
    [[noreturn]] DataSourceDescription getDataSourceDescription() const override;

    bool isReference() const override;
    String getReferenceTarget() const override;
private:
    String reference_target;
};

}
