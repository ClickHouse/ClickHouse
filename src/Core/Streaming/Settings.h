#pragma once

#include <Analyzer/IQueryTreeNode.h>

#include <Core/Streaming/CursorTree_fwd.h>

#include <chrono>

namespace DB
{

struct WatermarkSettings
{
    String column;
    QueryTreeNodePtr expression;
    std::chrono::milliseconds idle_timeout;
};
using WatermarkSettingsPtr = std::shared_ptr<WatermarkSettings>;

struct StreamingSettings
{
    CursorTreeNodePtr cursor;
    WatermarkSettingsPtr watermark;
};
using StreamingSettingsPtr = std::shared_ptr<StreamingSettings>;

///////////////////////////////////////////////////////////////////////////////////

bool isIdleExpired(
    const std::chrono::steady_clock::time_point & now,
    const std::chrono::steady_clock::time_point & last_active,
    const WatermarkSettingsPtr & watermark);

}
