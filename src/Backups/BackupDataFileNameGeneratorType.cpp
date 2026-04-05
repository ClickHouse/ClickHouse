#include <Backups/BackupDataFileNameGeneratorType.h>
#include <Common/Exception.h>

#include <boost/range/adaptor/map.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

IMPLEMENT_SETTING_ENUM(
    BackupDataFileNameGeneratorType,
    ErrorCodes::BAD_ARGUMENTS,
    {{"", BackupDataFileNameGeneratorType::FirstFileName}, {"checksum", BackupDataFileNameGeneratorType::Checksum}})
}
