#include <Parsers/queryToString.h>

#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/TableJoin.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int AMBIGUOUS_COLUMN_NAME;
    extern const int SYNTAX_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

void CollectJoinOnKeysMatcher::Data::addJoinKeys(const ASTPtr & left_ast, const ASTPtr & right_ast,
                                                 const std::pair<size_t, size_t> & table_no)
{
    ASTPtr left = left_ast->clone();
    ASTPtr right = right_ast->clone();

    if (table_no.first == 1 || table_no.second == 2)
        analyzed_join.addOnKeys(left, right);
    else if (table_no.first == 2 || table_no.second == 1)
        analyzed_join.addOnKeys(right, left);
    else
        throw Exception("Cannot detect left and right JOIN keys. JOIN ON section is ambiguous.",
                        ErrorCodes::AMBIGUOUS_COLUMN_NAME);
    has_some = true;
}

void CollectJoinOnKeysMatcher::Data::addAsofJoinKeys(const ASTPtr & left_ast, const ASTPtr & right_ast,
                                                     const std::pair<size_t, size_t> & table_no, const ASOF::Inequality & inequality)
{
    if (table_no.first == 1 || table_no.second == 2)
    {
        asof_left_key = left_ast->clone();
        asof_right_key = right_ast->clone();
        analyzed_join.setAsofInequality(inequality);
    }
    else if (table_no.first == 2 || table_no.second == 1)
    {
        asof_left_key = right_ast->clone();
        asof_right_key = left_ast->clone();
        analyzed_join.setAsofInequality(ASOF::reverseInequality(inequality));
    }
}

void CollectJoinOnKeysMatcher::Data::asofToJoinKeys()
{
    if (!asof_left_key || !asof_right_key)
        throw Exception("No inequality in ASOF JOIN ON section.", ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
    addJoinKeys(asof_left_key, asof_right_key, {1, 2});
}

void CollectJoinOnKeysMatcher::visit(const ASTFunction & func, const ASTPtr & ast, Data & data)
{
    if (func.name == "and")
        return; /// go into children

    if (func.name == "or")
        throw Exception("JOIN ON does not support OR. Unexpected '" + queryToString(ast) + "'", ErrorCodes::NOT_IMPLEMENTED);

    ASOF::Inequality inequality = ASOF::getInequality(func.name);
    if (func.name == "equals" || inequality != ASOF::Inequality::None)
    {
        if (func.arguments->children.size() != 2)
            throw Exception("Function " + func.name + " takes two arguments, got '" + func.formatForErrorMessage() + "' instead",
                            ErrorCodes::SYNTAX_ERROR);
    }
    else
        throw Exception("Expected equality or inequality, got '" + queryToString(ast) + "'", ErrorCodes::INVALID_JOIN_ON_EXPRESSION);

    if (func.name == "equals")
    {
        ASTPtr left = func.arguments->children.at(0);
        ASTPtr right = func.arguments->children.at(1);
        auto table_numbers = getTableNumbers(ast, left, right, data);
        data.addJoinKeys(left, right, table_numbers);
    }
    else if (inequality != ASOF::Inequality::None)
    {
        if (!data.is_asof)
            throw Exception("JOIN ON inequalities are not supported. Unexpected '" + queryToString(ast) + "'",
                            ErrorCodes::NOT_IMPLEMENTED);

        if (data.asof_left_key || data.asof_right_key)
            throw Exception("ASOF JOIN expects exactly one inequality in ON section. Unexpected '" + queryToString(ast) + "'",
                            ErrorCodes::INVALID_JOIN_ON_EXPRESSION);

        ASTPtr left = func.arguments->children.at(0);
        ASTPtr right = func.arguments->children.at(1);
        auto table_numbers = getTableNumbers(ast, left, right, data);

        data.addAsofJoinKeys(left, right, table_numbers, inequality);
    }
}

void CollectJoinOnKeysMatcher::getIdentifiers(const ASTPtr & ast, std::vector<const ASTIdentifier *> & out)
{
    if (const auto * func = ast->as<ASTFunction>())
    {
        if (func->name == "arrayJoin")
            throw Exception("Not allowed function in JOIN ON. Unexpected '" + queryToString(ast) + "'",
                            ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
    }
    else if (const auto * ident = ast->as<ASTIdentifier>())
    {
        if (IdentifierSemantic::getColumnName(*ident))
            out.push_back(ident);
        return;
    }

    for (const auto & child : ast->children)
        getIdentifiers(child, out);
}

std::pair<size_t, size_t> CollectJoinOnKeysMatcher::getTableNumbers(const ASTPtr & expr, const ASTPtr & left_ast, const ASTPtr & right_ast,
                                                                    Data & data)
{
    std::vector<const ASTIdentifier *> left_identifiers;
    std::vector<const ASTIdentifier *> right_identifiers;

    getIdentifiers(left_ast, left_identifiers);
    getIdentifiers(right_ast, right_identifiers);

    if (left_identifiers.empty() || right_identifiers.empty())
    {
        throw Exception("Not equi-join ON expression: " + queryToString(expr) + ". No columns in one of equality side.",
                        ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
    }

    size_t left_idents_table = getTableForIdentifiers(left_identifiers, data);
    size_t right_idents_table = getTableForIdentifiers(right_identifiers, data);

    if (left_idents_table && left_idents_table == right_idents_table)
    {
        auto left_name = queryToString(*left_identifiers[0]);
        auto right_name = queryToString(*right_identifiers[0]);

        throw Exception("In expression " + queryToString(expr) + " columns " + left_name + " and " + right_name
            + " are from the same table but from different arguments of equal function", ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
    }

    return std::make_pair(left_idents_table, right_idents_table);
}

const ASTIdentifier * CollectJoinOnKeysMatcher::unrollAliases(const ASTIdentifier * identifier, const Aliases & aliases)
{
    if (identifier->supposedToBeCompound())
        return identifier;

    UInt32 max_attempts = 100;
    for (auto it = aliases.find(identifier->name()); it != aliases.end();)
    {
        const ASTIdentifier * parent = identifier;
        identifier = it->second->as<ASTIdentifier>();
        if (!identifier)
            break; /// not a column alias
        if (identifier == parent)
            break; /// alias to itself with the same name: 'a as a'
        if (identifier->supposedToBeCompound())
            break; /// not an alias. Break to prevent cycle through short names: 'a as b, t1.b as a'

        it = aliases.find(identifier->name());
        if (!max_attempts--)
            throw Exception("Cannot unroll aliases for '" + identifier->name() + "'", ErrorCodes::LOGICAL_ERROR);
    }

    return identifier;
}

/// @returns 1 if identifiers belongs to left table, 2 for right table and 0 if unknown. Throws on table mix.
/// Place detected identifier into identifiers[0] if any.
size_t CollectJoinOnKeysMatcher::getTableForIdentifiers(std::vector<const ASTIdentifier *> & identifiers, const Data & data)
{
    size_t table_number = 0;

    for (auto & ident : identifiers)
    {
        const ASTIdentifier * identifier = unrollAliases(ident, data.aliases);
        if (!identifier)
            continue;

        /// Column name could be cropped to a short form in TranslateQualifiedNamesVisitor.
        /// In this case it saves membership in IdentifierSemantic.
        auto opt = IdentifierSemantic::getMembership(*identifier);
        size_t membership = opt ? (*opt + 1) : 0;

        if (!membership)
        {
            const String & name = identifier->name();
            bool in_left_table = data.left_table.hasColumn(name);
            bool in_right_table = data.right_table.hasColumn(name);

            if (in_left_table && in_right_table)
            {
                /// Relax ambiguous check for multiple JOINs
                if (auto original_name = IdentifierSemantic::uncover(*identifier))
                {
                    auto match = IdentifierSemantic::canReferColumnToTable(*original_name, data.right_table.table);
                    if (match == IdentifierSemantic::ColumnMatch::NoMatch)
                        in_right_table = false;
                    in_left_table = !in_right_table;
                }
                else
                    throw Exception("Column '" + name + "' is ambiguous", ErrorCodes::AMBIGUOUS_COLUMN_NAME);
            }

            if (in_left_table)
                membership = 1;
            if (in_right_table)
                membership = 2;
        }

        if (membership && table_number == 0)
        {
            table_number = membership;
            std::swap(ident, identifiers[0]); /// move first detected identifier to the first position
        }

        if (membership && membership != table_number)
        {
            throw Exception("Invalid columns in JOIN ON section. Columns "
                        + identifiers[0]->getAliasOrColumnName() + " and " + ident->getAliasOrColumnName()
                        + " are from different tables.", ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
        }
    }

    return table_number;
}

}
