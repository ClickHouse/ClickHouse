#pragma once

#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/ordered_index.hpp>

namespace DB
{

class CNFQuery
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
        ASTPtr ast;

        /// for set
        bool operator<(const AtomicFormula & rhs) const
        {
            return ast->getTreeHash(/*ignore_aliases=*/ true) == rhs.ast->getTreeHash(/*ignore_aliases=*/ true)
                ? negative < rhs.negative
                : ast->getTreeHash(/*ignore_aliases=*/ true) < rhs.ast->getTreeHash(/*ignore_aliases=*/ true);
        }

        bool operator==(const AtomicFormula & rhs) const
        {
            return negative == rhs.negative &&
                ast->getTreeHash(/*ignore_aliases=*/ true) == rhs.ast->getTreeHash(/*ignore_aliases=*/ true) &&
                ast->getColumnName() == rhs.ast->getColumnName();
        }
    };

    using OrGroup = CNFSet<AtomicFormula>;
    using AndGroup = CNFSet<OrGroup>;

    CNFQuery(AndGroup && statements_) : statements(std::move(statements_)) { } /// NOLINT

    template <typename P>
    CNFQuery & filterAlwaysTrueGroups(P predicate_is_unknown)  /// delete always true groups
    {
        AndGroup filtered;
        for (const auto & or_group : statements)
        {
            if (predicate_is_unknown(or_group))
                filtered.push_back(or_group);
        }
        std::swap(statements, filtered);
        return *this;
    }

    template <typename P>
    CNFQuery & filterAlwaysFalseAtoms(P predicate_is_unknown)  /// delete always false atoms
    {
        AndGroup filtered;
        for (const auto & or_group : statements)
        {
            OrGroup filtered_group;
            for (auto ast : or_group)
            {
                if (predicate_is_unknown(ast))
                    filtered_group.push_back(ast);
            }
            if (!filtered_group.empty())
                filtered.push_back(filtered_group);
            else
            {
                /// all atoms false -> group false -> CNF false
                filtered.clear();
                filtered_group.clear();
                filtered_group.push_back(AtomicFormula{false, std::make_shared<ASTLiteral>(static_cast<UInt8>(0))});
                filtered.push_back(filtered_group);
                std::swap(statements, filtered);
                return *this;
            }
        }
        std::swap(statements, filtered);
        return *this;
    }

    template <typename F>
    const CNFQuery & iterateGroups(F func) const
    {
        for (const auto & group : statements)
            func(group);
        return *this;
    }

    CNFQuery & appendGroup(AndGroup&& and_group)
    {
        for (auto && or_group : and_group)
            statements.emplace_back(or_group);
        return *this;
    }

    template <typename F>
    CNFQuery & transformGroups(F func)
    {
        AndGroup result;
        for (const auto & group : statements)
        {
            auto new_group = func(group);
            if (!new_group.empty())
                result.push_back(std::move(new_group));
        }
        std::swap(statements, result);
        return *this;
    }

    template <typename F>
    CNFQuery & transformAtoms(F func)
    {
        transformGroups([func](const OrGroup & group) -> OrGroup
                        {
                            OrGroup result;
                            for (const auto & atom : group)
                            {
                                auto new_atom = func(atom);
                                if (new_atom.ast)
                                    result.push_back(std::move(new_atom));
                            }
                            return result;
                        });
        return *this;
    }

    const AndGroup & getStatements() const { return statements; }

    std::string dump() const;

    /// Converts != -> NOT =; <,>= -> (NOT) <; >,<= -> (NOT) <= for simpler matching
    CNFQuery & pullNotOutFunctions();
    /// Revert pullNotOutFunctions actions
    CNFQuery & pushNotInFunctions();

    /// (a OR b OR ...) AND (NOT a OR b OR ...) -> (b OR ...)
    CNFQuery & reduce();

private:
    AndGroup statements;
};

class TreeCNFConverter
{
public:
    static constexpr size_t DEFAULT_MAX_GROWTH_MULTIPLIER = 20;
    static constexpr size_t MAX_ATOMS_WITHOUT_CHECK = 200;

    /// @max_growth_multiplier means that it's allowed to grow size of formula only
    /// in that amount of times. It's needed to avoid exponential explosion of formula.
    /// CNF of boolean formula with N clauses can have 2^N clauses.
    /// If amount of atomic formulas will be exceeded nullopt will be returned.
    /// 0 - means unlimited.
    static std::optional<CNFQuery> tryConvertToCNF(
        const ASTPtr & query, size_t max_growth_multiplier = DEFAULT_MAX_GROWTH_MULTIPLIER);

    static CNFQuery toCNF(
        const ASTPtr & query, size_t max_growth_multiplier = DEFAULT_MAX_GROWTH_MULTIPLIER);

    static ASTPtr fromCNF(const CNFQuery & cnf);
};

void pushNotIn(CNFQuery::AtomicFormula & atom);

template <typename TAndGroup>
TAndGroup reduceOnceCNFStatements(const TAndGroup & groups)
{
    TAndGroup result;
    for (const auto & group : groups)
    {
        using GroupType = std::decay_t<decltype(group)>;
        GroupType copy(group);
        bool inserted = false;
        for (const auto & atom : group)
        {
            copy.template get<1>().erase(atom);
            using AtomType = std::decay_t<decltype(atom)>;
            AtomType negative_atom(atom);
            negative_atom.negative = !atom.negative;
            copy.push_back(negative_atom);

            if (groups.template get<1>().contains(copy))
            {
                copy.template get<1>().erase(negative_atom);
                result.push_back(copy);
                inserted = true;
                break;
            }

            copy.template get<1>().erase(negative_atom);
            copy.push_back(atom);
        }
        if (!inserted)
            result.push_back(group);
    }
    return result;
}

template <typename TOrGroup>
bool isCNFGroupSubset(const TOrGroup & left, const TOrGroup & right)
{
    if (left.size() > right.size())
        return false;
    for (const auto & elem : left)
        if (!right.template get<1>().contains(elem))
            return false;
    return true;
}

template <typename TAndGroup>
TAndGroup filterCNFSubsets(const TAndGroup & groups)
{
    TAndGroup result;
    for (const auto & group : groups)
    {
        bool insert = true;

        for (const auto & other_group : groups)
        {
            if (isCNFGroupSubset(other_group, group) && group != other_group)
            {
                insert = false;
                break;
            }
        }

        if (insert)
            result.push_back(group);
    }
    return result;
}

}
