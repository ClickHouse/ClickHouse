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
    bool operator==(const WatermarkSettings & rhs) const;
};
using WatermarkSettingsPtr = std::shared_ptr<WatermarkSettings>;

struct StreamSettings
{
    /// If true, read only the first snapshot and then finish (do not subscribe for updates).
    bool subscribe_for_updates = true;
    /// If true, do not sort each snapshot by cursor; ordering holds only between snapshots.
    bool unordered = false;
    CursorTreeNodePtr cursor;
    WatermarkSettingsPtr watermark;

public:
    std::shared_ptr<StreamSettings> clone() const;
    bool operator==(const StreamSettings & rhs) const;
};
using StreamSettingsPtr = std::shared_ptr<StreamSettings>;

}
