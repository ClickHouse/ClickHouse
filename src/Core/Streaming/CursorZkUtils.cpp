#include <filesystem>

#include <Core/Streaming/CursorZkUtils.h>
#include <Core/Streaming/CursorTree.h>
#include "Common/ZooKeeper/Types.h"

namespace DB
{

static const String kCursorsPrefix = "/clickhouse/cursors/";

String getCursorNodePath(const String & keeper_key)
{
    return std::filesystem::path(kCursorsPrefix) / keeper_key;
}

CursorTreeNodePtr extractOrInitCursor(zkutil::ZooKeeperPtr zk, const String & keeper_key, const CursorTreeNodePtr & init)
{
    const String cursor_node_path = getCursorNodePath(keeper_key);

    String data;
    bool exists = zk->tryGet(cursor_node_path, data);

    if (exists)
        return buildCursorTree(data);

    zk->createAncestors(cursor_node_path);
    zk->create(cursor_node_path, cursorTreeToString(init), zkutil::CreateMode::Persistent);

    return init;
}

}
