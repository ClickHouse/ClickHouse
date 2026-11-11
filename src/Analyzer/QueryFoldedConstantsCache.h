#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Columns/IColumn_fwd.h>

#include <map>
#include <memory>
#include <mutex>

namespace DB
{

/// The value every call to a function that is non-deterministic but constant within a query (`now`, `now64`,
/// `randConstant`, `joinGet`, ...) was folded to, keyed by the call's tree hash, as the analyzer deduplicates such
/// calls (see `resolveFunction`).
///
/// One cache serves one execution of a query: `Context::makeQueryContext` creates it, and every analysis of the
/// query shares it - those of views and subqueries by their own interpreters, and the analysis that builds the plan
/// with parallel replicas `automatic_parallel_replicas_mode` compares against. The comparison hands that plan the
/// index analysis of the single-node plan, which is only correct when both read the same values. Each analysis
/// builds its own functions, so the value is what has to be shared: `joinGet` reads its table when it is executed.
///
/// Only materialized values are kept. A function or a set would keep a context alive, and the context owns this.
class QueryFoldedConstantsCache
{
public:
    ColumnConstPtr tryGet(const IQueryTreeNode::Hash & hash) const
    {
        std::lock_guard lock(mutex);
        auto it = values.find(hash);
        return it == values.end() ? nullptr : it->second;
    }

    /// Keeps `value` for `hash` unless another analysis has kept one first, and returns the kept one.
    ColumnConstPtr insert(const IQueryTreeNode::Hash & hash, ColumnConstPtr value)
    {
        std::lock_guard lock(mutex);
        return values.emplace(hash, std::move(value)).first->second;
    }

private:
    mutable std::mutex mutex;
    std::map<IQueryTreeNode::Hash, ColumnConstPtr> values;
};

using QueryFoldedConstantsCachePtr = std::shared_ptr<QueryFoldedConstantsCache>;

}
