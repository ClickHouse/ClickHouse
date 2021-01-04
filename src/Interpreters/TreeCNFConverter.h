#pragma once

#include <Parsers/IAST_fwd.h>
#include <vector>
#include <set>
#include <unordered_map>

namespace DB
{

class CNFQuery
{
public:
    using OrGroup = std::set<ASTPtr>; // Add NOT container???
    using AndGroup = std::set<OrGroup>;

    CNFQuery(AndGroup && statements_)
        : statements(std::move(statements_)) {}

    template <typename P>
    void filterGroups(P predicate) {
        AndGroup filtered;
        for (const auto & or_group : statements)
        {
            if (predicate(or_group))
                filtered.insert(or_group);
        }
        std::swap(statements, filtered);
    }

    template <typename P>
    void filterAtoms(P predicate) {
        AndGroup filtered;
        for (const auto & or_group : statements)
        {
            OrGroup filtered_group;
            for (auto ast : or_group) {
                if (predicate(ast))
                    filtered_group.insert(ast);
            }
            if (!filtered_group.empty())
                filtered.insert(filtered_group);
        }
        std::swap(statements, filtered);
    }

    const AndGroup & getStatements() const { return statements; }

    std::string dump() const;

private:
    AndGroup statements;
};

class TreeCNFConverter
{
public:

    static CNFQuery toCNF(const ASTPtr & query);

    static ASTPtr fromCNF(const CNFQuery & cnf);
};

}
