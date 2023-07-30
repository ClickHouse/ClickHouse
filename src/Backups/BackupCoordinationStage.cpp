#include <Backups/BackupCoordinationStage.h>
#include <fmt/format.h>


namespace DB
{

String BackupCoordinationStage::formatGatheringMetadata(size_t pass)
{
    return fmt::format("{} ({})", GATHERING_METADATA, pass);
}

}
