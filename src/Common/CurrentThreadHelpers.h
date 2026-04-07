#pragma once

#include <Core/LogsLevel.h>

namespace DB
{
bool currentThreadHasGroup();
LogsLevel currentThreadLogsLevel();
}
