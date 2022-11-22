#pragma once

#include <Core/Block.h>
#include <Storages/IStorage_fwd.h>


namespace DB
{

class QueryPlan;

class Set;
using SetPtr = std::shared_ptr<Set>;

/// Information on what to do when executing a subquery in the [GLOBAL] IN/JOIN section.
struct SubqueryForSet
{
    SubqueryForSet();
    ~SubqueryForSet();
    SubqueryForSet(SubqueryForSet &&) noexcept;
    SubqueryForSet & operator=(SubqueryForSet &&) noexcept;

    /// The source is obtained using the InterpreterSelectQuery subquery.
    std::unique_ptr<QueryPlan> source;

    /// If set, build it from result.
    SetPtr set;

    /// If set, put the result into the table.
    /// This is a temporary table for transferring to remote servers for distributed query processing.
    StoragePtr table;
};

/// ID of subquery -> what to do with it.
using SubqueriesForSets = std::unordered_map<String, SubqueryForSet>;

}
