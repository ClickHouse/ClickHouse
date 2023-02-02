#pragma once

#include <Analyzer/IQueryTreePass.h>
#include <Storages/IStorage_fwd.h>
#include <Analyzer/TableNode.h>

namespace DB
{


class AutoFinalOnQueryPass final : public IQueryTreePass
{
public:
    String getName() override;

    String getDescription() override;

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;
};

}
