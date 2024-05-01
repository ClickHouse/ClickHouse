#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>

#include <Core/Streaming/CursorData.h>
#include <Core/Streaming/CursorTree.h>

namespace DB
{

String getCursorNodePath(const String & keeper_key);

CursorTreeNodePtr extractOrInitCursor(zkutil::ZooKeeperPtr zk, const String & keeper_key, const CursorTreeNodePtr & init);

template <class ZooKeeperPtr>
void getUpdateCursorOps(ZooKeeperPtr zk, Coordination::Requests & ops, const CursorDataMap & cursors)
{
    std::vector<String> paths;
    std::vector<CursorData> keeper_cursors;

    for (const auto & [_, data] : cursors)
    {
        if (data.keeper_key.has_value())
        {
            paths.push_back(getCursorNodePath(data.keeper_key.value()));
            keeper_cursors.push_back(data);
        }
    }

    auto responses = zk->get(paths);
    responses.waitForResponses();

    for (size_t i = 0; i < keeper_cursors.size(); ++i)
    {
        const auto response = responses[i];
        if (response.error != Coordination::Error::ZOK)
            throw Coordination::Exception::fromPath(response.error, paths[i]);

        CursorTreeNodePtr saved_cursor = buildCursorTree(response.data);
        mergeCursors(saved_cursor, keeper_cursors[i].tree);

        ops.emplace_back(zkutil::makeSetRequest(paths[i], cursorTreeToString(saved_cursor), response.stat.version));
    }
}

}
