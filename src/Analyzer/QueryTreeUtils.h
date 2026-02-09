#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

void replaceStorageInQueryTree(
    QueryTreeNodePtr & query_tree,
    const ContextPtr & context,
    const StoragePtr & storage);

}
