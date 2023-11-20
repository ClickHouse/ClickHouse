#include <Parsers/ASTIdentifier.h>
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

bool isDeterminedIdentifier(JoinIdentifierPos pos)
{
    return pos == JoinIdentifierPos::Left || pos == JoinIdentifierPos::Right;
}

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

void CollectJoinOnKeysMatcher::Data::addJoinKeys(const ASTPtr & left_ast, const ASTPtr & right_ast, JoinIdentifierPosPair table_pos, bool null_safe_comparison)
{
    ASTPtr left = left_ast->clone();
    ASTPtr right = right_ast->clone();

    if (isLeftIdentifier(table_pos.first) && isRightIdentifier(table_pos.second))
        analyzed_join.addOnKeys(left, right, null_safe_comparison);
    else if (isRightIdentifier(table_pos.first) && isLeftIdentifier(table_pos.second))
        analyzed_join.addOnKeys(right, left, null_safe_comparison);
    else
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot detect left and right JOIN keys. JOIN ON section is ambiguous.");
}

void CollectJoinOnKeysMatcher::Data::addAsofJoinKeys(const ASTPtr & left_ast, const ASTPtr & right_ast,
                                                     JoinIdentifierPosPair table_pos, const ASOFJoinInequality & inequality)
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
        analyzed_join.setAsofInequality(reverseASOFJoinInequality(inequality));
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
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "No inequality in ASOF JOIN ON section.");
    addJoinKeys(asof_left_key, asof_right_key, {JoinIdentifierPos::Left, JoinIdentifierPos::Right}, false);
}

void CollectJoinOnKeysMatcher::visit(const ASTIdentifier & ident, const ASTPtr & ast, CollectJoinOnKeysMatcher::Data & data)
{
    if (auto expr_from_table = getTableForIdentifiers(ast, false, data); isDeterminedIdentifier(expr_from_table))
        data.analyzed_join.addJoinCondition(ast, isLeftIdentifier(expr_from_table));
    else
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Unexpected identifier '{}' in JOIN ON section", ident.name());
}

void CollectJoinOnKeysMatcher::visit(const ASTFunction & func, const ASTPtr & ast, Data & data)
{
    if (func.name == "and")
        return; /// go into children

    ASOFJoinInequality inequality = getASOFJoinInequality(func.name);

    if (func.name == "equals" || func.name == "isNotDistinctFrom" || inequality != ASOFJoinInequality::None)
    {
        if (func.arguments->children.size() != 2)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Function {} takes two arguments, got '{}' instead",
                            func.name, func.formatForErrorMessage());
    }

    if (func.name == "equals" || func.name == "isNotDistinctFrom")
    {
        ASTPtr left = func.arguments->children.at(0);
        ASTPtr right = func.arguments->children.at(1);
        auto table_numbers = getTableNumbers(left, right, data);

        if (table_numbers.first == table_numbers.second)
        {
            if (!isDeterminedIdentifier(table_numbers.first))
                throw Exception(ErrorCodes::AMBIGUOUS_COLUMN_NAME,
                    "Ambiguous columns in expression '{}' in JOIN ON section", queryToString(ast));
            data.analyzed_join.addJoinCondition(ast, isLeftIdentifier(table_numbers.first));
            return;
        }

        if ((isLeftIdentifier(table_numbers.first) && isRightIdentifier(table_numbers.second)) ||
            (isRightIdentifier(table_numbers.first) && isLeftIdentifier(table_numbers.second)))
        {
            bool null_safe_comparison = func.name == "isNotDistinctFrom";
            data.addJoinKeys(left, right, table_numbers, null_safe_comparison);
            return;
        }
    }


    if (auto expr_from_table = getTableForIdentifiers(ast, false, data); isDeterminedIdentifier(expr_from_table))
    {
        data.analyzed_join.addJoinCondition(ast, isLeftIdentifier(expr_from_table));
        return;
    }

    if (data.is_asof && inequality != ASOFJoinInequality::None)
    {
        if (data.asof_left_key || data.asof_right_key)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF JOIN expects exactly one inequality in ON section. "
                            "Unexpected '{}'", queryToString(ast));

        ASTPtr left = func.arguments->children.at(0);
        ASTPtr right = func.arguments->children.at(1);
        auto table_numbers = getTableNumbers(left, right, data);

        data.addAsofJoinKeys(left, right, table_numbers, inequality);
        return;
    }

    throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Unsupported JOIN ON conditions. Unexpected '{}'",
                    queryToString(ast));
}

void CollectJoinOnKeysMatcher::getIdentifiers(const ASTPtr & ast, std::vector<const ASTIdentifier *> & out)
{
    if (const auto * func = ast->as<ASTFunction>())
    {
        if (func->name == "arrayJoin")
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Not allowed function in JOIN ON. Unexpected '{}'",
                            queryToString(ast));
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

JoinIdentifierPosPair CollectJoinOnKeysMatcher::getTableNumbers(const ASTPtr & left_ast, const ASTPtr & right_ast, Data & data)
{
    auto left_idents_table = getTableForIdentifiers(left_ast, true, data);
    auto right_idents_table = getTableForIdentifiers(right_ast, true, data);

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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unroll aliases for '{}'", identifier->name());
    }

    return identifier;
}

/// @returns Left or right table identifiers belongs to.
/// Place detected identifier into identifiers[0] if any.
JoinIdentifierPos CollectJoinOnKeysMatcher::getTableForIdentifiers(const ASTPtr & ast, bool throw_on_table_mix, const Data & data)
{
    std::vector<const ASTIdentifier *> identifiers;
    getIdentifiers(ast, identifiers);
    if (identifiers.empty())
        return JoinIdentifierPos::NotColumn;

    JoinIdentifierPos table_number = JoinIdentifierPos::Unknown;

    for (auto & ident : identifiers)
    {
        const ASTIdentifier * identifier = unrollAliases(ident, data.aliases);
        if (!identifier)
            continue;

        /// Column name could be cropped to a short form in TranslateQualifiedNamesVisitor.
        /// In this case it saves membership in IdentifierSemantic.
        JoinIdentifierPos membership = JoinIdentifierPos::Unknown;
        if (auto opt = IdentifierSemantic::getMembership(*identifier); opt.has_value())
        {
            if (*opt == 0)
                membership = JoinIdentifierPos::Left;
            else if (*opt == 1)
                membership = JoinIdentifierPos::Right;
            else
                throw DB::Exception(ErrorCodes::AMBIGUOUS_COLUMN_NAME,
                                    "Position of identifier {} can't be deteminated.",
                                    identifier->name());
        }

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
                    throw Exception(ErrorCodes::AMBIGUOUS_COLUMN_NAME, "Column '{}' is ambiguous", name);
            }

            if (in_left_table)
                membership = JoinIdentifierPos::Left;
            if (in_right_table)
                membership = JoinIdentifierPos::Right;
        }

        if (membership != JoinIdentifierPos::Unknown && table_number == JoinIdentifierPos::Unknown)
        {
            table_number = membership;
            std::swap(ident, identifiers[0]); /// move first detected identifier to the first position
        }

        if (membership != JoinIdentifierPos::Unknown && membership != table_number)
        {
            if (throw_on_table_mix)
                throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Invalid columns in JOIN ON section. "
                            "Columns {} and {} are from different tables.",
                            identifiers[0]->getAliasOrColumnName(), ident->getAliasOrColumnName());
            return JoinIdentifierPos::Unknown;
        }
    }

    return table_number;
}

}
