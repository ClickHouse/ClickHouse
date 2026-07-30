#pragma once

#include <Core/Streaming/CursorTree.h>

#include <Parsers/IAST_fwd.h>

#include <chrono>

namespace DB
{

struct WatermarkSettings
{
    String column;
    ASTPtr expression;
    std::chrono::milliseconds idle_timeout{0};

public:
    std::shared_ptr<WatermarkSettings> clone() const;
};
using WatermarkSettingsPtr = std::shared_ptr<WatermarkSettings>;

struct StreamSettings
{
    CursorTreeNodePtr cursor;
    WatermarkSettingsPtr watermark;

public:
    std::shared_ptr<StreamSettings> clone() const;
};
using StreamSettingsPtr = std::shared_ptr<StreamSettings>;

}
