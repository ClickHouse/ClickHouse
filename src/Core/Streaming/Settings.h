#pragma once

#include <Core/Streaming/CursorTree_fwd.h>

#include <chrono>

namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

struct WatermarkSettings
{
    String column;
    QueryTreeNodePtr expression;
    std::chrono::milliseconds idle_timeout{0};
};
using WatermarkSettingsPtr = std::shared_ptr<WatermarkSettings>;

struct StreamingSettings
{
    CursorTreeNodePtr cursor;
    WatermarkSettingsPtr watermark;
};
using StreamingSettingsPtr = std::shared_ptr<StreamingSettings>;

}
