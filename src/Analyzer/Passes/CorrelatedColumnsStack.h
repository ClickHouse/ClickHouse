#pragma once

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ListNode.h>
#include <Common/SipHash.h>

#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace DB
{

/// Keyed on the source NODE, not the StorageID: every `file()`/`s3()`/`url()` resolves to
/// `_table_function.<name>`, so two such sources in one query would otherwise share a key.
struct ColumnInSource
{
    const IQueryTreeNode * source = nullptr;
    String column_name;

    bool operator==(const ColumnInSource & rhs) const = default;
};

struct ColumnInSourceHash
{
    size_t operator()(const ColumnInSource & key) const
    {
        SipHash hash;
        hash.update(reinterpret_cast<uintptr_t>(key.source));
        hash.update(key.column_name);
        return hash.get64();
    }
};

template <typename Value>
using ColumnInSourceMap = std::unordered_map<ColumnInSource, Value, ColumnInSourceHash>;
using ColumnInSourceSet = std::unordered_set<ColumnInSource, ColumnInSourceHash>;

inline ColumnInSource makeColumnInSource(const QueryTreeNodePtr & column_source, const String & column_name)
{
    return ColumnInSource{static_cast<const IQueryTreeNode *>(column_source.get()), column_name};
}

/// The correlated columns of every subquery on the way down, one entry per query or union level.
/// A column of an outer query that a subquery lists among its correlated columns must never be
/// replaced by a subcolumn inside that subquery: the list would keep naming the whole column,
/// and decorrelation would then look for the subcolumn in a join that carries only the whole
/// column - the query fails with `NOT_FOUND_COLUMN_IN_BLOCK`.
class CorrelatedColumnsStack
{
public:
    void enter(const ListNode & correlated_columns)
    {
        ColumnInSourceSet columns;
        for (const auto & correlated_column : correlated_columns.getNodes())
        {
            const auto * column_node = correlated_column->as<ColumnNode>();
            if (!column_node)
                continue;

            columns.insert(makeColumnInSource(column_node->getColumnSource(), column_node->getColumnName()));
        }

        stack.push_back(std::move(columns));
    }

    void leave() { stack.pop_back(); }

    [[nodiscard]] bool contains(const ColumnInSource & column_in_source) const
    {
        for (const auto & columns : stack)
            if (columns.contains(column_in_source))
                return true;

        return false;
    }

private:
    std::vector<ColumnInSourceSet> stack;
};

}
