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
            ASTPtr res = func->arguments->children.front();
            for (size_t i = 1; i < func->arguments->children.size(); ++i)
            {
                res = makeASTFunction(func->name, res, func->arguments->children[i]);
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
                func->arguments->children[0],
                func->arguments->children[1]);
        }

        auto * new_func = node->as<ASTFunction>();
        for (auto & child : new_func->arguments->children)
            traversePushNot(child, add_negation);
    }
    else if (func && func->name == "not")
    {
        /// delete NOT
        node = func->arguments->children[0];

        traversePushNot(node, !add_negation);
    }
    else
    {
        if (add_negation)
            node = makeASTFunction("not", node);
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
                      makeASTFunction("or", a, b),
                      makeASTFunction("or", a, c));

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
    else
    {
        or_group.insert(node);
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
            or_groups.push_back(*group.begin());
        else if (group.size() > 1)
        {
            or_groups.push_back(makeASTFunction("or"));
            auto * func = or_groups.back()->as<ASTFunction>();
            for (const auto & ast : group)
                func->arguments->children.push_back(ast->clone());
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

void pullNotOut(ASTPtr & node)
{
    static const std::map<std::string, std::string> inverse_relations = {
        {"notEquals", "equals"},
        {"greaterOrEquals", "less"},
        {"greater", "lessOrEquals"},
        {"notIn", "in"},
        {"notLike", "like"},
        {"notEmpty", "empty"},
    };

    auto * func = node->as<ASTFunction>();
    if (!func)
        return;
    if (auto it = inverse_relations.find(func->name); it != std::end(inverse_relations))
    {
        /// inverse func
        node = node->clone();
        auto * new_func = node->as<ASTFunction>();
        new_func->name = it->second;
        /// add not
        node = makeASTFunction("not", node);
    }
}

void pushNotIn(ASTPtr & node)
{
    static const std::map<std::string, std::string> inverse_relations = {
        {"equals", "notEquals"},
        {"less", "greaterOrEquals"},
        {"lessOrEquals", "greater"},
        {"in", "notIn"},
        {"like", "notLike"},
        {"empty", "notEmpty"},
    };

    auto * func = node->as<ASTFunction>();
    if (!func)
        return;
    if (auto it = inverse_relations.find(func->name); it != std::end(inverse_relations))
    {
        /// inverse func
        node = node->clone();
        auto * new_func = node->as<ASTFunction>();
        new_func->name = it->second;
        /// add not
        node = makeASTFunction("not", node);
    }
}

CNFQuery & CNFQuery::pullNotOutFunctions()
{
    transformAtoms([](const ASTPtr & node) -> ASTPtr
                 {
                     auto * func = node->as<ASTFunction>();
                     if (!func)
                        return node;
                     ASTPtr result = node->clone();
                     if (func->name == "not")
                         pullNotOut(func->arguments->children.front());
                     else
                         pullNotOut(result);
                     traversePushNot(result, false);
                     return result;
                 });
    return *this;
}

CNFQuery & CNFQuery::pushNotInFuntions()
{
    transformAtoms([](const ASTPtr & node) -> ASTPtr
                   {
                       auto * func = node->as<ASTFunction>();
                       if (!func)
                           return node;
                       ASTPtr result = node->clone();
                       if (func->name == "not")
                           pushNotIn(func->arguments->children.front());
                       traversePushNot(result, false);
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
        for (const auto & ast : group)
        {
            if (!first_in_group)
                res << " OR ";
            first_in_group = false;
            res << ast->getColumnName();
        }
        res << ")";
    }

    return res.str();
}

}
