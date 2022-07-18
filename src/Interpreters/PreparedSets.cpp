#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

PreparedSetKey PreparedSetKey::forLiteral(const IAST & ast, DataTypes types_)
{
    /// Remove LowCardinality types from type list because Set doesn't support LowCardinality keys now,
    ///   just converts LowCardinality to ordinary types.
    for (auto & type : types_)
        type = recursiveRemoveLowCardinality(type);

    PreparedSetKey key;
    key.ast_hash = ast.getTreeHash();
    key.types = std::move(types_);
    return key;
}

PreparedSetKey PreparedSetKey::forSubquery(const IAST & ast)
{
    PreparedSetKey key;
    key.ast_hash = ast.getTreeHash();
    return key;
}

bool PreparedSetKey::operator==(const PreparedSetKey & other) const
{
    if (ast_hash != other.ast_hash)
        return false;

    if (types.size() != other.types.size())
        return false;

    for (size_t i = 0; i < types.size(); ++i)
    {
        if (!types[i]->equals(*other.types[i]))
            return false;
    }

    return true;
}

SubqueryForSet & PreparedSets::createOrGetSubquery(const String & subquery_id, const PreparedSetKey & key)
{
    if (auto subqiery_it = subqueries.find(subquery_id); subqiery_it != subqueries.end())
    {
        /// If you already created a Set with the same subquery / table for another ast
        /// In that case several PreparedSetKey would share same subquery and set
        /// Not sure if it's really possible case (maybe for distributed query when set was filled by external table?)
        if (subqiery_it->second.set)
            sets[key] = subqiery_it->second.set;
        return subqiery_it->second;
    }

    return subqueries.emplace(subquery_id, sets[key]).first->second;
}

void PreparedSets::setSet(const PreparedSetKey & key, SetPtr set_)
{
    sets[key] = std::move(set_);
}

SetPtr & PreparedSets::getSet(const PreparedSetKey & key)
{
    return sets[key];
}

PreparedSets::SubqueriesForSets PreparedSets::moveSubqueries()
{
    auto res = std::move(subqueries);
    subqueries = SubqueriesForSets();
    return res;
}

bool PreparedSets::empty() const
{
    return sets.empty();
}

SubqueryForSet::SubqueryForSet(SetPtr & set_) : set(set_) {}

SubqueryForSet::~SubqueryForSet() = default;

SubqueryForSet::SubqueryForSet(SubqueryForSet &&) noexcept = default;

};
