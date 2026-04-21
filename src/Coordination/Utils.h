#pragma once

#include <Common/Exception.h>

namespace DB
{

/// Find nodes that are outdated and all children are outdated
std::vector<String> findOldNodes(const std::vector<std::pair<String, bool>> & nodes);

}
