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

struct StreamSettings
{
    CursorTreeNodePtr cursor;
    WatermarkSettingsPtr watermark;
};
using StreamSettingsPtr = std::shared_ptr<StreamSettings>;

}
