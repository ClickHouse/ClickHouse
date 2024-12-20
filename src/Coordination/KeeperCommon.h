#pragma once

#include <base/StringRef.h>
#include "Common/Logger.h"

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class KeeperContext;
using KeeperContextPtr = std::shared_ptr<KeeperContext>;

StringRef parentNodePath(StringRef path);

StringRef getBaseNodeName(StringRef path);

inline static constexpr std::string_view tmp_keeper_file_prefix = "tmp_";

void moveFileBetweenDisks(
    DiskPtr disk_from,
    const std::string & path_from,
    DiskPtr disk_to,
    const std::string & path_to,
    std::function<void()> before_file_remove_op,
    LoggerPtr logger,
    const KeeperContextPtr & keeper_context);

}
