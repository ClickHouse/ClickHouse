#include <Interpreters/TreeCNFConverter.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/checkStackSize.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int TOO_MANY_TEMPORARY_COLUMNS;
}

namespace
{

bool isLogicalFunction(const ASTFunction & func)
{
    return func.name == "and" || func.name == "or" || func.name == "not";
}

size_t countAtoms(const ASTPtr & node)
{
    checkStackSize();
    if (node->as<ASTIdentifier>() || node->as<ASTLiteral>())
        return 1;

    const auto * func = node->as<ASTFunction>();
    if (func && !isLogicalFunction(*func))
        return 1;

    size_t num_atoms = 0;
    for (const auto & child : node->children)
        num_atoms += countAtoms(child);
    return num_atoms;
}

/// Splits AND(a, b, c) to AND(a, AND(b, c)) for AND/OR
void splitMultiLogic(ASTPtr & node)
{
    checkStackSize();
    auto * func = node->as<ASTFunction>();

    if (func && (func->name == "and" || func->name == "or"))
    {
        if (func->arguments->children.size() < 2)
            throw Exception("Bad AND or OR function. Expected at least 2 arguments", ErrorCodes::INCORRECT_QUERY);

        if (func->arguments->children.size() > 2)
        {
            ASTPtr res = func->arguments->children.front()->clone();
            for (auto it = ++func->arguments->children.begin(); it != func->arguments->children.end(); ++it)
                res = makeASTFunction(func->name, res, *it);

            node = res;
        }

        auto * new_func = node->as<ASTFunction>();
        for (auto & child : new_func->arguments->children)
            splitMultiLogic(child);
    }
    else if (func && func->name == "not")
    {
        for (auto & child : func->arguments->children)
            splitMultiLogic(child);
    }
}

/// Push NOT to leafs, remove NOT NOT ...
void traversePushNot(ASTPtr & node, bool add_negation)
{
    checkStackSize();
    auto * func = node->as<ASTFunction>();

    if (func && (func->name == "and" || func->name == "or"))
    {
        if (add_negation)
        {
            if (func->arguments->children.size() != 2)
                throw Exception("Bad AND or OR function. Expected at least 2 arguments", ErrorCodes::LOGICAL_ERROR);

            /// apply De Morgan's Law
            node = makeASTFunction(
                (func->name == "and" ? "or" : "and"),
                func->arguments->children.front()->clone(),
                func->arguments->children.back()->clone());
        }

        auto * new_func = node->as<ASTFunction>();
        for (auto & child : new_func->arguments->children)
            traversePushNot(child, add_negation);
    }
    else if (func && func->name == "not")
    {
        if (func->arguments->children.size() != 1)
            throw Exception("Bad NOT function. Expected 1 argument", ErrorCodes::INCORRECT_QUERY);
        /// delete NOT
        node = func->arguments->children.front()->clone();

        traversePushNot(node, !add_negation);
    }
    else
    {
        if (add_negation)
            node = makeASTFunction("not", node->clone());
    }
}

/// Push Or inside And (actually pull AND to top)
bool traversePushOr(ASTPtr & node, size_t num_atoms, size_t max_atoms)
{
    if (max_atoms && num_atoms > max_atoms)
        return false;

    checkStackSize();
    auto * func = node->as<ASTFunction>();

    if (func && (func->name == "or" || func->name == "and"))
    {
        for (auto & child : func->arguments->children)
            if (!traversePushOr(child, num_atoms, max_atoms))
                return false;
    }

    if (func && func->name == "or")
    {
        assert(func->arguments->children.size() == 2);
        ASTPtr and_node = nullptr;
        ASTPtr other_node = nullptr;

        auto * and_func = func->arguments->children.front()->as<ASTFunction>();
        if (and_func && and_func->name == "and")
        {
            and_node = func->arguments->children.front();
            other_node = func->arguments->children.back();
        }
        else if (and_func = func->arguments->children.back()->as<ASTFunction>(); and_func && and_func->name == "and")
        {
            and_node = func->arguments->children.back();
            other_node = func->arguments->children.front();
        }

        if (!and_node)
            return true;

        auto a = other_node;
        auto b = and_func->arguments->children.front();
        auto c = and_func->arguments->children.back();

        /// apply the distributive law ( a or (b and c) -> (a or b) and (a or c) )
        node = makeASTFunction(
            "and",
            makeASTFunction("or", a->clone(), b),
            makeASTFunction("or", a, c));

        /// Count all atoms from 'a', because it was cloned.
        num_atoms += countAtoms(a);
        return traversePushOr(node, num_atoms, max_atoms);
    }

    return true;
}

/// transform ast into cnf groups
void traverseCNF(const ASTPtr & node, CNFQuery::AndGroup & and_group, CNFQuery::OrGroup & or_group)
{
    checkStackSize();

    auto * func = node->as<ASTFunction>();
    if (func && func->name == "and")
    {
        for (auto & child : func->arguments->children)
        {
            CNFQuery::OrGroup group;
            traverseCNF(child, and_group, group);
            if (!group.empty())
                and_group.insert(std::move(group));
        }
    }
    else if (func && func->name == "or")
    {
        for (auto & child : func->arguments->children)
        {
            traverseCNF(child, and_group, or_group);
        }
    }
    else if (func && func->name == "not")
    {
        if (func->arguments->children.size() != 1)
            throw Exception("Bad NOT function. Expected 1 argument", ErrorCodes::INCORRECT_QUERY);
        or_group.insert(CNFQuery::AtomicFormula{true, func->arguments->children.front()});
    }
    else
    {
        or_group.insert(CNFQuery::AtomicFormula{false, node});
    }
}

void traverseCNF(const ASTPtr & node, CNFQuery::AndGroup & result)
{
    CNFQuery::OrGroup or_group;
    traverseCNF(node, result, or_group);
    if (!or_group.empty())
        result.insert(or_group);
}

}

std::optional<CNFQuery> TreeCNFConverter::tryConvertToCNF(
    const ASTPtr & query, size_t max_growth_multiplier)
{
    auto cnf = query->clone();
    size_t num_atoms = countAtoms(cnf);

    splitMultiLogic(cnf);
    traversePushNot(cnf, false);

    size_t max_atoms = max_growth_multiplier
        ? std::max(MAX_ATOMS_WITHOUT_CHECK, num_atoms * max_growth_multiplier)
        : 0;

    if (!traversePushOr(cnf, num_atoms, max_atoms))
        return {};

    CNFQuery::AndGroup and_group;
    traverseCNF(cnf, and_group);

    CNFQuery result{std::move(and_group)};

    return result;
}

CNFQuery TreeCNFConverter::toCNF(
    const ASTPtr & query, size_t max_growth_multiplier)
{
    auto cnf = tryConvertToCNF(query, max_growth_multiplier);
    if (!cnf)
        throw Exception(ErrorCodes::TOO_MANY_TEMPORARY_COLUMNS,
            "Cannot convert expression '{}' to CNF, because it produces to many clauses."
            "Size of boolean formula in CNF can be exponential of size of source formula.");

    return *cnf;
}

ASTPtr TreeCNFConverter::fromCNF(const CNFQuery & cnf)
{
    const auto & groups = cnf.getStatements();
    if (groups.empty())
        return nullptr;

    ASTs or_groups;
    for (const auto & group : groups)
    {
        if (group.size() == 1)
        {
            if ((*group.begin()).negative)
                or_groups.push_back(makeASTFunction("not", (*group.begin()).ast->clone()));
            else
                or_groups.push_back((*group.begin()).ast->clone());
        }
        else if (group.size() > 1)
        {
            or_groups.push_back(makeASTFunction("or"));
            auto * func = or_groups.back()->as<ASTFunction>();
            for (const auto & atom : group)
            {
                if (atom.negative)
                    func->arguments->children.push_back(makeASTFunction("not", atom.ast->clone()));
                else
                    func->arguments->children.push_back(atom.ast->clone());
            }
        }
    }

    if (or_groups.size() == 1)
        return or_groups.front();

    ASTPtr res = makeASTFunction("and");
    auto * func = res->as<ASTFunction>();
    for (const auto & group : or_groups)
        func->arguments->children.push_back(group);

    return res;
}

static void pushPullNotInAtom(CNFQuery::AtomicFormula & atom, const std::unordered_map<std::string, std::string> & inverse_relations)
{
    auto * func = atom.ast->as<ASTFunction>();
    if (!func)
        return;
    if (auto it = inverse_relations.find(func->name); it != std::end(inverse_relations))
    {
        /// inverse func
        atom.ast = atom.ast->clone();
        auto * new_func = atom.ast->as<ASTFunction>();
        new_func->name = it->second;
        /// add not
        atom.negative = !atom.negative;
    }
}

static void pullNotOut(CNFQuery::AtomicFormula & atom)
{
    static const std::unordered_map<std::string, std::string> inverse_relations = {
        {"notEquals", "equals"},
        {"greaterOrEquals", "less"},
        {"greater", "lessOrEquals"},
        {"notIn", "in"},
        {"notLike", "like"},
        {"notEmpty", "empty"},
    };

    pushPullNotInAtom(atom, inverse_relations);
}

void pushNotIn(CNFQuery::AtomicFormula & atom)
{
    if (!atom.negative)
        return;

    static const std::unordered_map<std::string, std::string> inverse_relations = {
        {"equals", "notEquals"},
        {"less", "greaterOrEquals"},
        {"lessOrEquals", "greater"},
        {"in", "notIn"},
        {"like", "notLike"},
        {"empty", "notEmpty"},
        {"notEquals", "equals"},
        {"greaterOrEquals", "less"},
        {"greater", "lessOrEquals"},
        {"notIn", "in"},
        {"notLike", "like"},
        {"notEmpty", "empty"},
    };

    pushPullNotInAtom(atom, inverse_relations);
}

CNFQuery & CNFQuery::pullNotOutFunctions()
{
    transformAtoms([](const AtomicFormula & atom) -> AtomicFormula
                    {
                        AtomicFormula result{atom.negative, atom.ast->clone()};
                        pullNotOut(result);
                        return result;
                    });
    return *this;
}

CNFQuery & CNFQuery::pushNotInFuntions()
{
    transformAtoms([](const AtomicFormula & atom) -> AtomicFormula
                   {
                       AtomicFormula result{atom.negative, atom.ast->clone()};
                       pushNotIn(result);
                       return result;
                   });
    return *this;
}

namespace
{
    CNFQuery::AndGroup reduceOnce(const CNFQuery::AndGroup & groups)
    {
        CNFQuery::AndGroup result;
        for (const CNFQuery::OrGroup & group : groups)
        {
            CNFQuery::OrGroup copy(group);
            bool inserted = false;
            for (const CNFQuery::AtomicFormula & atom : group)
            {
                copy.erase(atom);
                CNFQuery::AtomicFormula negative_atom(atom);
                negative_atom.negative = !atom.negative;
                copy.insert(negative_atom);

                if (groups.contains(copy))
                {
                    copy.erase(negative_atom);
                    result.insert(copy);
                    inserted = true;
                    break;
                }

                copy.erase(negative_atom);
                copy.insert(atom);
            }
            if (!inserted)
                result.insert(group);
        }
        return result;
    }

    bool isSubset(const CNFQuery::OrGroup & left, const CNFQuery::OrGroup & right)
    {
        if (left.size() > right.size())
            return false;
        for (const auto & elem : left)
            if (!right.contains(elem))
                return false;
        return true;
    }

    CNFQuery::AndGroup filterSubsets(const CNFQuery::AndGroup & groups)
    {
        CNFQuery::AndGroup result;
        for (const CNFQuery::OrGroup & group : groups)
        {
            bool insert = true;

            for (const CNFQuery::OrGroup & other_group : groups)
            {
                if (isSubset(other_group, group) && group != other_group)
                {
                    insert = false;
                    break;
                }
            }

            if (insert)
                result.insert(group);
        }
        return result;
    }
}

CNFQuery & CNFQuery::reduce()
{
    while (true)
    {
        AndGroup new_statements = reduceOnce(statements);
        if (statements == new_statements)
        {
            statements = filterSubsets(statements);
            return *this;
        }
        else
            statements = new_statements;
    }
}

std::string CNFQuery::dump() const
{
    WriteBufferFromOwnString res;
    bool first = true;
    for (const auto & group : statements)
    {
        if (!first)
            res << " AND ";
        first = false;
        res << "(";
        bool first_in_group = true;
        for (const auto & atom : group)
        {
            if (!first_in_group)
                res << " OR ";
            first_in_group = false;
            if (atom.negative)
                res << " NOT ";
            res << atom.ast->getColumnName();
        }
        res << ")";
    }

    return res.str();
}

}
