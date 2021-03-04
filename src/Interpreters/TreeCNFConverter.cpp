#include <Interpreters/TreeCNFConverter.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>

#include <Poco/Logger.h>

namespace DB
{

/// Splits AND(a, b, c) to AND(a, AND(b, c)) for AND/OR
void splitMultiLogic(ASTPtr & node)
{
    auto * func = node->as<ASTFunction>();

    if (func && (func->name == "and" || func->name == "or"))
    {
        if (func->arguments->children.size() > 2)
        {
            ASTPtr res = func->arguments->children.front()->clone();
            for (size_t i = 1; i < func->arguments->children.size(); ++i)
            {
                res = makeASTFunction(func->name, res, func->arguments->children[i]->clone());
            }
            node = std::move(res);
        }

        auto * new_func = node->as<ASTFunction>();
        for (auto & child : new_func->arguments->children)
            splitMultiLogic(child);
    }
}

/// Push NOT to leafs, remove NOT NOT ...
void traversePushNot(ASTPtr & node, bool add_negation)
{
    auto * func = node->as<ASTFunction>();

    if (func && (func->name == "and" || func->name == "or"))
    {
        if (add_negation) {
            /// apply De Morgan's Law
            node = makeASTFunction(
                (func->name == "and" ? "or" : "and"),
                func->arguments->children[0]->clone(),
                func->arguments->children[1]->clone());
        }

        auto * new_func = node->as<ASTFunction>();
        for (auto & child : new_func->arguments->children)
            traversePushNot(child, add_negation);
    }
    else if (func && func->name == "not")
    {
        /// delete NOT
        node = func->arguments->children[0]->clone();

        traversePushNot(node, !add_negation);
    }
    else
    {
        if (add_negation)
            node = makeASTFunction("not", node->clone());
    }
}

void findOrs(ASTPtr & node, std::vector<std::reference_wrapper<ASTPtr>> & ors)
{
    auto * func = node->as<ASTFunction>();

    if (func && func->name == "or")
        ors.push_back(node);

    if (func)
    {
        for (auto & child : func->arguments->children)
            findOrs(child, ors);
    }
}

/// Push Or inside And (actually pull AND to top)
void pushOr(ASTPtr & query)
{
    std::vector<std::reference_wrapper<ASTPtr>> ors;
    findOrs(query, ors);

    while (!ors.empty())
    {
        std::reference_wrapper<ASTPtr> or_node = ors.back();
        ors.pop_back();

        auto * or_func = or_node.get()->as<ASTFunction>();
        ASSERT(or_func)
        ASSERT(or_func->name == "or")

        /// find or upper than and
        size_t and_node_id = or_func->arguments->children.size();
        for (size_t i = 0; i < or_func->arguments->children.size(); ++i)
        {
            auto & child = or_func->arguments->children[i];
            auto * and_func = child->as<ASTFunction>();
            if (and_func->name == "and")
            {
                and_node_id = i;
            }
        }
        if (and_node_id == or_func->arguments->children.size())
            continue;
        const size_t other_node_id = 1 - and_node_id;

        auto and_func = or_func->arguments->children[and_node_id]->as<ASTFunction>();
        ASSERT(and_func)
        ASSERT(and_func->name == "and")

        auto a = or_func->arguments->children[other_node_id];
        auto b = and_func->arguments->children[0];
        auto c = and_func->arguments->children[1];

        /// apply the distributive law ( a or (b and c) -> (a or b) and (a or c) )
        or_node.get() = makeASTFunction(
                      "and",
                      makeASTFunction("or", a->clone(), b->clone()),
                      makeASTFunction("or", a->clone(), c->clone()));

        /// add new ors to stack
        auto * new_func = or_node.get()->as<ASTFunction>();
        for (auto & new_or : new_func->arguments->children)
            ors.push_back(new_or);
    }
}

/// transform ast into cnf groups
void traverseCNF(const ASTPtr & node, CNFQuery::AndGroup & and_group, CNFQuery::OrGroup & or_group)
{
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

CNFQuery TreeCNFConverter::toCNF(const ASTPtr & query)
{
    auto cnf = query->clone();

    splitMultiLogic(cnf);
    traversePushNot(cnf, false);
    pushOr(cnf);

    CNFQuery::AndGroup and_group;
    traverseCNF(cnf, and_group);

    CNFQuery result{std::move(and_group)};

    Poco::Logger::get("CNF CONVERSION").information("DONE: " + result.dump());
    return result;
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
                if ((*group.begin()).negative)
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

void pushPullNotInAtom(CNFQuery::AtomicFormula & atom, const std::map<std::string, std::string> & inverse_relations)
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

void pullNotOut(CNFQuery::AtomicFormula & atom)
{
    static const std::map<std::string, std::string> inverse_relations = {
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

    static const std::map<std::string, std::string> inverse_relations = {
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

std::string CNFQuery::dump() const
{
    std::stringstream res;
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
