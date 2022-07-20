#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

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

SubqueryForSet & PreparedSets::createOrGetSubquery(const String & subquery_id, const PreparedSetKey & key, SetPtr set_)
{
    SubqueryForSet & subquery = subqueries[subquery_id];

    /// If you already created a Set with the same subquery / table for another ast
    /// In that case several PreparedSetKey would share same subquery and set
    /// Not sure if it's really possible case (maybe for distributed query when set was filled by external table?)
    if (subquery.set)
        sets[key] = subquery.set;
    else
        sets[key] = subquery.set = set_;
    return subquery;
}

SubqueryForSet & PreparedSets::getSubquery(const String & subquery_id) { return subqueries[subquery_id]; }

void PreparedSets::setSet(const PreparedSetKey & key, SetPtr set_) { sets[key] = set_; }

SetPtr & PreparedSets::getSet(const PreparedSetKey & key) { return sets[key]; }

std::vector<SetPtr> PreparedSets::getByTreeHash(IAST::Hash ast_hash)
{
    std::vector<SetPtr> res;
    for (const auto & it : this->sets)
    {
        if (it.first.ast_hash == ast_hash)
            res.push_back(it.second);
    }
    return res;
}

PreparedSets::SubqueriesForSets PreparedSets::moveSubqueries()
{
    auto res = std::move(subqueries);
    subqueries = SubqueriesForSets();
    return res;
}

bool PreparedSets::empty() const { return sets.empty(); }

SubqueryForSet::SubqueryForSet() = default;

SubqueryForSet::~SubqueryForSet() = default;

SubqueryForSet::SubqueryForSet(SubqueryForSet &&) noexcept = default;

void SubqueryForSet::setSource(InterpreterSelectWithUnionQuery & interpreter, StoragePtr table_)
{
    source = std::make_unique<QueryPlan>();
    interpreter.buildQueryPlan(*source);
    if (table_)
        table = table_;
}

bool SubqueryForSet::hasSource() const
{
    return source != nullptr;
}

QueryPlanPtr SubqueryForSet::moveSource()
{
    auto res = std::move(source);
    source = nullptr;
    return res;
}

};
