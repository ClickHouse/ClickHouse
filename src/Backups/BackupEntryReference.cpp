#include <Backups/BackupEntryReference.h>

namespace DB
{

namespace ErrorCodes
{

extern const int NOT_IMPLEMENTED;

}

BackupEntryReference::BackupEntryReference(std::string reference_target_)
    : reference_target(std::move(reference_target_))
{}

bool BackupEntryReference::isReference() const
{
    return true;
}

String BackupEntryReference::getReferenceTarget() const
{
    return reference_target;
}

UInt64 BackupEntryReference::getSize() const
{
    return 0;
}

UInt128 BackupEntryReference::getChecksum(const ReadSettings & /*read_settings*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Checksum not implemented for reference backup entries");
}

std::unique_ptr<SeekableReadBuffer> BackupEntryReference::getReadBuffer(const ReadSettings & /*read_settings*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reading not implemented for reference backup entries");
}

DataSourceDescription BackupEntryReference::getDataSourceDescription() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Data source description not implemented for reference backup entries");
}

}
