#pragma once

#include <Analyzer/IQueryTreePass.h>

#include <Interpreters/Context_fwd.h>

namespace DB
{

/** Query tree pass manager provide functionality to register and run passes
  * on query tree.
  */
class QueryTreePassManager : public WithContext
{
public:
    explicit QueryTreePassManager(ContextPtr context_);

    /// Get registered passes
    const std::vector<QueryTreePassPtr> & getPasses() const
    {
        return passes;
    }

    /// Add query tree pass
    void addPass(QueryTreePassPtr pass);

    /// Run query tree passes on query tree
    void run(QueryTreeNodePtr & query_tree_node);

    /// Run only query tree passes responsible to name resolution.
    void runOnlyResolve(QueryTreeNodePtr & query_tree_node);

    /** Run query tree passes on query tree up to up_to_pass_index.
      * Throws exception if up_to_pass_index is greater than passes size.
      */
    void run(QueryTreeNodePtr & query_tree_node, size_t up_to_pass_index);

    /// Dump query tree passes
    void dump(WriteBuffer & buffer);

    /** Dump query tree passes to up_to_pass_index.
      * Throws exception if up_to_pass_index is greater than passes size.
      */
    void dump(WriteBuffer & buffer, size_t up_to_pass_index);

private:
    std::vector<QueryTreePassPtr> passes;
};

/// `add_random_order_injection` controls whether InjectRandomOrderIfNoOrderByPass is added
/// (the pass itself is a no-op unless the setting `inject_random_order_for_select_without_order_by` is enabled).
/// It must be disabled for queries processed only up to an intermediate stage, see the comment at the call site.
void addQueryTreePasses(QueryTreePassManager & manager, bool only_analyze = false, bool add_random_order_injection = true);

}
