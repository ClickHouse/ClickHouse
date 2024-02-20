#pragma once

#include <Analyzer/HashUtils.h>
#include <Analyzer/IQueryTreeNode.h>

#include <Common/SipHash.h>

#include <Interpreters/Context_fwd.h>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/ordered_index.hpp>

namespace DB::Analyzer
{

class CNF
{
public:
    template <typename T>
    using CNFSet = boost::multi_index_container<
        T,
        boost::multi_index::
            indexed_by<boost::multi_index::sequenced<>, boost::multi_index::ordered_unique<boost::multi_index::identity<T>>>>;

    struct AtomicFormula
    {
        bool negative = false;
        QueryTreeNodePtrWithHash node_with_hash;

        bool operator==(const AtomicFormula & rhs) const;
        bool operator<(const AtomicFormula & rhs) const;
    };

    /// Keep insertion order so that optimization result is stable
    using OrGroup = CNFSet<AtomicFormula>;
    using AndGroup = CNFSet<OrGroup>;

    std::string dump() const;

    static constexpr size_t DEFAULT_MAX_GROWTH_MULTIPLIER = 20;
    static constexpr size_t MAX_ATOMS_WITHOUT_CHECK = 200;

    CNF & transformAtoms(std::function<AtomicFormula(const AtomicFormula &)> fn);
    CNF & transformGroups(std::function<OrGroup(const OrGroup &)> fn);

    CNF & filterAlwaysTrueGroups(std::function<bool(const OrGroup &)> predicate);
    CNF & filterAlwaysFalseAtoms(std::function<bool(const AtomicFormula &)> predicate);

    CNF & reduce();

    void appendGroup(const AndGroup & and_group);

    /// Convert "NOT fn" to a single node representing inverse of "fn"
    CNF & pushNotIntoFunctions(const ContextPtr & context);
    CNF & pullNotOutFunctions(const ContextPtr & context);

    static AtomicFormula pushNotIntoFunction(const AtomicFormula & atom, const ContextPtr & context);

    explicit CNF(AndGroup statements_);

    static std::optional<CNF> tryBuildCNF(const QueryTreeNodePtr & node, ContextPtr context, size_t max_growth_multiplier = DEFAULT_MAX_GROWTH_MULTIPLIER);
    static CNF toCNF(const QueryTreeNodePtr & node, ContextPtr context, size_t max_growth_multiplier = DEFAULT_MAX_GROWTH_MULTIPLIER);

    QueryTreeNodePtr toQueryTree() const;

    const auto & getStatements() const
    {
        return statements;
    }
private:
    AndGroup statements;
};

}
