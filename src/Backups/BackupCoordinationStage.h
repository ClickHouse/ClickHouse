#pragma once

#include <base/types.h>


namespace DB
{

namespace BackupCoordinationStage
{
    /// This stage is set after concurrency check so ensure we dont start other backup/restores
    /// when concurrent backup/restores are not allowed
    constexpr const char * SCHEDULED_TO_START = "scheduled to start";

    /// Finding all tables and databases which we're going to put to the backup and collecting their metadata.
    constexpr const char * GATHERING_METADATA = "gathering metadata";

    String formatGatheringMetadata(int attempt_no);

    /// Making temporary hard links and prepare backup entries.
    constexpr const char * EXTRACTING_DATA_FROM_TABLES = "extracting data from tables";

    /// Running special tasks for replicated tables which can also prepare some backup entries.
    constexpr const char * RUNNING_POST_TASKS = "running post-tasks";

    /// Building information about all files which will be written to a backup.
    constexpr const char * BUILDING_FILE_INFOS = "building file infos";

    /// Writing backup entries to the backup and removing temporary hard links.
    constexpr const char * WRITING_BACKUP = "writing backup";

    /// Finding databases and tables in the backup which we're going to restore.
    constexpr const char * FINDING_TABLES_IN_BACKUP = "finding tables in backup";

    /// Loading system access tables and then checking if the current user has enough access to restore.
    constexpr const char * CHECKING_ACCESS_RIGHTS = "checking access rights";

    /// Creating databases or finding them and checking their definitions.
    constexpr const char * CREATING_DATABASES = "creating databases";

    /// Creating tables or finding them and checking their definition.
    constexpr const char * CREATING_TABLES = "creating tables";

    /// Inserting restored data to tables.
    constexpr const char * INSERTING_DATA_TO_TABLES = "inserting data to tables";

    /// Coordination stage meaning that a host finished its work.
    constexpr const char * COMPLETED = "completed";

    /// Coordination stage meaning that backup/restore has failed due to an error
    /// Check '/error' for the error message
    constexpr const char * ERROR = "error";
}

}
