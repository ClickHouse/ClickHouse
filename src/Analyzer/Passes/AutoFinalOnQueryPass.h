#pragma once

#include <Analyzer/IQueryTreePass.h>
#include <Storages/IStorage_fwd.h>
#include <Analyzer/TableNode.h>

namespace DB
{

/** Automatically applies final modifier to table expressions in queries if it is supported and if user level final setting is set.
  *
  * Example: SELECT id, value FROM test_table;
  * Result: SELECT id, value FROM test_table FINAL;
  */
class AutoFinalOnQueryPass final : public IQueryTreePass
{
public:
    String getName() override
    {
        return "AutoFinalOnQueryPass";
    }

    String getDescription() override
    {
        return "Automatically applies final modifier to table expressions in queries if it is supported and if user level final setting is set";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
