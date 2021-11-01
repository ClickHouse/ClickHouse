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
    extern const int LOGICAL_ERROR;
}

namespace
{

bool isLeftIdentifier(JoinIdentifierPos pos)
{
    /// Unknown identifiers  considered as left, we will try to process it on later stages
    /// Usually such identifiers came from `ARRAY JOIN ... AS ...`
    return pos == JoinIdentifierPos::Left || pos == JoinIdentifierPos::Unknown;
}

bool isRightIdentifier(JoinIdentifierPos pos)
{
    return pos == JoinIdentifierPos::Right;
}

}

void CollectJoinOnKeysMatcher::Data::addJoinKeys(const ASTPtr & left_ast, const ASTPtr & right_ast, JoinIdentifierPosPair table_pos)
{
    ASTPtr left = left_ast->clone();
    ASTPtr right = right_ast->clone();

    if (isLeftIdentifier(table_pos.first) && isRightIdentifier(table_pos.second))
        analyzed_join.addOnKeys(left, right);
    else if (isRightIdentifier(table_pos.first) && isLeftIdentifier(table_pos.second))
        analyzed_join.addOnKeys(right, left);
    else
        throw Exception("Cannot detect left and right JOIN keys. JOIN ON section is ambiguous.",
                        ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
}

void CollectJoinOnKeysMatcher::Data::addAsofJoinKeys(const ASTPtr & left_ast, const ASTPtr & right_ast,
                                                     JoinIdentifierPosPair table_pos, const ASOF::Inequality & inequality)
{
    if (isLeftIdentifier(table_pos.first) && isRightIdentifier(table_pos.second))
    {
        asof_left_key = left_ast->clone();
        asof_right_key = right_ast->clone();
        analyzed_join.setAsofInequality(inequality);
    }
    else if (isRightIdentifier(table_pos.first) && isLeftIdentifier(table_pos.second))
    {
        asof_left_key = right_ast->clone();
        asof_right_key = left_ast->clone();
        analyzed_join.setAsofInequality(ASOF::reverseInequality(inequality));
    }
    else
    {
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                        "Expressions {} and {} are from the same table but from different arguments of equal function in ASOF JOIN",
                        queryToString(left_ast), queryToString(right_ast));
    }
}

void CollectJoinOnKeysMatcher::Data::asofToJoinKeys()
{
    if (!asof_left_key || !asof_right_key)
        throw Exception("No inequality in ASOF JOIN ON section.", ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
    addJoinKeys(asof_left_key, asof_right_key, {JoinIdentifierPos::Left, JoinIdentifierPos::Right});
}

void CollectJoinOnKeysMatcher::visit(const ASTIdentifier & ident, const ASTPtr & ast, CollectJoinOnKeysMatcher::Data & data)
{
    if (auto expr_from_table = getTableForIdentifiers(ast, false, data); expr_from_table != JoinIdentifierPos::Unknown)
        data.analyzed_join.addJoinCondition(ast, isLeftIdentifier(expr_from_table));
    else
        throw Exception("Unexpected identifier '" + ident.name() + "' in JOIN ON section",
                        ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
}

void CollectJoinOnKeysMatcher::visit(const ASTFunction & func, const ASTPtr & ast, Data & data)
{
    if (func.name == "and")
        return; /// go into children

    ASOF::Inequality inequality = ASOF::getInequality(func.name);
    if (func.name == "equals" || inequality != ASOF::Inequality::None)
    {
        if (func.arguments->children.size() != 2)
            throw Exception("Function " + func.name + " takes two arguments, got '" + func.formatForErrorMessage() + "' instead",
                            ErrorCodes::SYNTAX_ERROR);
    }

    if (func.name == "equals")
    {
        ASTPtr left = func.arguments->children.at(0);
        ASTPtr right = func.arguments->children.at(1);
        auto table_numbers = getTableNumbers(left, right, data);
        if (table_numbers.first == table_numbers.second)
        {
            if (table_numbers.first == JoinIdentifierPos::Unknown)
                throw Exception("Ambiguous column in expression '" + queryToString(ast) + "' in JOIN ON section",
                                ErrorCodes::AMBIGUOUS_COLUMN_NAME);
            data.analyzed_join.addJoinCondition(ast, isLeftIdentifier(table_numbers.first));
            return;
        }

        if (table_numbers.first != JoinIdentifierPos::NotApplicable && table_numbers.second != JoinIdentifierPos::NotApplicable)
        {
            data.addJoinKeys(left, right, table_numbers);
            return;
        }
    }

    if (auto expr_from_table = getTableForIdentifiers(ast, false, data); expr_from_table != JoinIdentifierPos::Unknown)
    {
        data.analyzed_join.addJoinCondition(ast, isLeftIdentifier(expr_from_table));
        return;
    }

    if (data.is_asof && inequality != ASOF::Inequality::None)
    {
        if (data.asof_left_key || data.asof_right_key)
            throw Exception("ASOF JOIN expects exactly one inequality in ON section. Unexpected '" + queryToString(ast) + "'",
                            ErrorCodes::INVALID_JOIN_ON_EXPRESSION);

        ASTPtr left = func.arguments->children.at(0);
        ASTPtr right = func.arguments->children.at(1);
        auto table_numbers = getTableNumbers(left, right, data);

        data.addAsofJoinKeys(left, right, table_numbers, inequality);
        return;
    }

    throw Exception("Unsupported JOIN ON conditions. Unexpected '" + queryToString(ast) + "'",
                    ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
}


JoinIdentifierPosPair CollectJoinOnKeysMatcher::getTableNumbers(const ASTPtr & left_ast, const ASTPtr & right_ast, Data & data)
{
    auto left_idents_table = getTableForIdentifiers(left_ast, true, data);
    auto right_idents_table = getTableForIdentifiers(right_ast, true, data);

    return std::make_pair(left_idents_table, right_idents_table);
}

static const ASTIdentifier * unrollAliases(const ASTIdentifier * identifier, const Aliases & aliases)
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

JoinIdentifierPos getTableForIdentifier(const ASTIdentifier * ident, const Aliases & aliases)
{
    const ASTIdentifier * identifier = unrollAliases(ident, aliases);
    if (!identifier)
        return JoinIdentifierPos::NotApplicable;

    /// Column name could be cropped to a short form in TranslateQualifiedNamesVisitor.
    /// In this case it saves membership in IdentifierSemantic.
    JoinIdentifierPos membership = JoinIdentifierPos::Unknown;
    if (auto opt = IdentifierSemantic::getMembership(*identifier); opt.has_value())
    {
        if (*opt == 0)
            membership = JoinIdentifierPos::Left;
        else if (*opt == 1)
            membership = JoinIdentifierPos::Right;
    }
    return membership;
}

/// @returns Left or right table identifiers belongs to.
JoinIdentifierPos CollectJoinOnKeysMatcher::getTableForIdentifiers(const ASTPtr & ast, bool throw_on_table_mix, const Data & data)
{
    auto ident_pred = [](const ASTPtr & node) -> bool
    {
        if (const auto * func = node->as<ASTFunction>(); func && func->name == "arrayJoin")
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Not allowed function in JOIN ON: '{}'", queryToString(node));

        const auto * ident = node->as<ASTIdentifier>();
        return !(ident && IdentifierSemantic::getColumnName(*ident)->empty());
    };

    std::vector<const ASTIdentifier *> identifiers = getIdentifiers(ast, ident_pred);

    if (identifiers.empty())
        return JoinIdentifierPos::NotApplicable;

    JoinIdentifierPos table_number = JoinIdentifierPos::Unknown;

    const ASTIdentifier * first_detected = nullptr;
    for (const auto * identifier : identifiers)
    {
        JoinIdentifierPos membership = getTableForIdentifier(identifier, data.aliases);
        if (membership == JoinIdentifierPos::NotApplicable)
            continue;

        if (membership == JoinIdentifierPos::Unknown)
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
                membership = JoinIdentifierPos::Left;
            if (in_right_table)
                membership = JoinIdentifierPos::Right;
        }

        if (membership != JoinIdentifierPos::Unknown && first_detected == nullptr)
        {
            table_number = membership;
            first_detected = identifier;
        }

        if (membership != JoinIdentifierPos::Unknown && membership != table_number)
        {
            if (throw_on_table_mix)
                throw Exception(
                    ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                    "Invalid columns in JOIN ON section. Columns {} and {} are from different tables",
                    first_detected ? first_detected->getAliasOrColumnName() : "???",
                    identifier->getAliasOrColumnName());

            return JoinIdentifierPos::Unknown;
        }
    }

    return table_number;
}

void joinConditionsWithAnd(ASTPtr & current_cond, const ASTPtr & new_cond)
{
    if (current_cond == nullptr)
        /// no conditions, set new one
        current_cond = new_cond;
    else if (const auto * func = current_cond->as<ASTFunction>(); func && func->name == "and")
        /// already have `and` in condition, just add new argument
        func->arguments->children.push_back(new_cond);
    else
        /// already have some conditions, unite it with `and`
        current_cond = makeASTFunction("and", current_cond, new_cond);
}

}
