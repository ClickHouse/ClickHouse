#pragma once

#include <Interpreters/Context_fwd.h>

#include <Analyzer/IQueryTreeNode.h>


namespace DB
{

/** After query tree is build it can be later processed by QueryTreePassManager.
  * This is abstract base class for all query tree passes.
  *
  * Query tree pass can make query tree modifications, after each pass query tree must be valid.
  * Query tree pass must perform be isolated and perform only necessary query tree modifications for doing its job.
  * Dependencies between passes must be avoided.
  */
class IQueryTreePass;
using QueryTreePassPtr = std::shared_ptr<IQueryTreePass>;
using QueryTreePasses = std::vector<QueryTreePassPtr>;

class IQueryTreePass
{
public:
    virtual ~IQueryTreePass() = default;

    /// Get query tree pass name
    virtual String getName() = 0;

    /// Get query tree pass description
    virtual String getDescription() = 0;

    /// Run pass over query tree
    virtual void run(QueryTreeNodePtr query_tree_node, ContextPtr context) = 0;

};

}
