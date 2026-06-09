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

    /// Run query tree passes on query tree.
    /// is_top_level=false suppresses the QueryTreeOptimizeMicroseconds timer for nested
    /// (re-entrant) analysis so it is not double-counted; its time is folded into the
    /// still-live outer timer instead.
    void run(QueryTreeNodePtr & query_tree_node, bool is_top_level = true);

    /// Run only query tree passes responsible to name resolution.
    void runOnlyResolve(QueryTreeNodePtr & query_tree_node);

    /** Run query tree passes on query tree up to up_to_pass_index.
      * Throws exception if up_to_pass_index is greater than passes size.
      */
    void runUntil(QueryTreeNodePtr & query_tree_node, size_t up_to_pass_index, bool is_top_level = true);

    /// Dump query tree passes
    void dump(WriteBuffer & buffer);

    /** Dump query tree passes to up_to_pass_index.
      * Throws exception if up_to_pass_index is greater than passes size.
      */
    void dump(WriteBuffer & buffer, size_t up_to_pass_index);

private:
    std::vector<QueryTreePassPtr> passes;
};

void addQueryTreePasses(QueryTreePassManager & manager, bool only_analyze = false, bool is_top_level = true);

}
