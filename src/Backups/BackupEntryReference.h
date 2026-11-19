#pragma once

#include <Backups/IBackupEntry.h>

namespace DB
{

/// Represents a reference to another backup entry.
class BackupEntryReference : public IBackupEntry
{
public:
    explicit BackupEntryReference(std::string reference_target_);

    [[nodiscard]] UInt64 getSize() const override;
    [[nodiscard]] UInt128 getChecksum(const ReadSettings & read_settings) const override;
    [[nodiscard]] std::unique_ptr<SeekableReadBuffer> getReadBuffer(const ReadSettings & read_settings) const override;
    [[nodiscard]] DataSourceDescription getDataSourceDescription() const override;

    [[nodiscard]] bool isReference() const override;
    [[nodiscard]] String getReferenceTarget() const override;
private:
    String reference_target;
};

}
