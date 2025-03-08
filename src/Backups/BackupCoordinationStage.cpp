#include <Backups/BackupCoordinationStage.h>

import fmt;

namespace DB
{

String BackupCoordinationStage::formatGatheringMetadata(int attempt_no)
{
    return fmt::format("{} ({})", GATHERING_METADATA, attempt_no);
}

}
