#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/queryToString.h>

#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/ActionsDAG.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>

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
    auto from_tables = getTableForIdentifiers(ast, data);
    if (from_tables.size() == 1)
    {
        auto from_table = from_tables.empty() ? JoinIdentifierPos::Right : *from_tables.begin();
        data.analyzed_join.addJoinCondition(ast, isLeftIdentifier(from_table));
    }
    else
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Unexpected identifier '{}' in JOIN ON section", ident.name());
}

bool CollectJoinOnKeysMatcher::isConstExpression(const IAST & func, Data & data)
{
    auto ast = func.clone();
    NamesAndTypesList all_cols;

    auto actions_dag = std::make_shared<ActionsDAG>(all_cols);
    DebugASTLog<false> log;
    NamesAndTypesList empty_agg_keys;
    ColumnNumbersList empty_agg_col_nums;
    NamesAndTypesList source_col;
    ActionsVisitor::Data visitor_data(
        data.context,
        {},
        0,
        source_col,
        std::move(actions_dag),
        {},
        true,
        true,
        false,
        {empty_agg_keys, empty_agg_col_nums, GroupByKind::NONE},
        false,
        false);
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions_dag = visitor_data.getActions();
    auto outputs = actions_dag->getOutputs();
    NameSet required_output{outputs.back()->result_name};
    actions_dag->removeUnusedActions(required_output);

    for (const auto & node : actions_dag->getNodes())
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION)
        {
            if (!node.is_deterministic)
                return false;
        }
        else if (node.type == ActionsDAG::ActionType::COLUMN)
        {
            if (!checkColumn<ColumnConst>(*node.column))
                return false;
        }
    }
    return true;
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

    bool is_asof_join_inequality = data.is_asof && inequality != ASOFJoinInequality::None;
    if (func.name == "equals" || func.name == "isNotDistinctFrom" || is_asof_join_inequality)
    {
        ASTPtr left = func.arguments->children.at(0);
        ASTPtr right = func.arguments->children.at(1);
        // auto table_numbers = getTableNumbers(left, right, data);
        auto left_tables = getTableForIdentifiers(left, data);
        auto right_tables = getTableForIdentifiers(right, data);

        if (left_tables.empty() && right_tables.empty())
        {
            if (isConstExpression(func, data))
                data.analyzed_join.addJoinCondition(ast, false);
            else
                throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Invalid join expression: {}", func.formatForErrorMessage());
        }
        else if (left_tables.size() == 1 && right_tables.empty())
        {
            data.analyzed_join.addJoinCondition(ast, true);
        }
        else if (left_tables.empty() && right_tables.size() == 1)
        {
            data.analyzed_join.addJoinCondition(ast, false);
        }
        else if (left_tables.size() == 1 && right_tables.size() ==1)
        {
            auto left_table_pos = *left_tables.begin();
            auto right_table_pos = *right_tables.begin();
            if (left_table_pos != right_table_pos)
            {
                JoinIdentifierPosPair table_numbers = std::make_pair(left_table_pos, right_table_pos);
                if (is_asof_join_inequality)
                {
                    if (data.has_asof)
                    {
                        throw Exception(
                            ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                            "JOIN {} ASOF JOIN expects exactly one inequality in ON section",
                            func.formatForErrorMessage());
                    }
                    data.has_asof = true;
                    data.addAsofJoinKeys(left, right, table_numbers, inequality);
                }
                else
                {
                    bool null_safe_comparison = func.name == "isNotDistinctFrom";
                    data.addJoinKeys(left, right, table_numbers, null_safe_comparison);
                }
            }
            else
            {
                data.analyzed_join.addJoinCondition(ast, isLeftIdentifier(left_table_pos));
            }
        }
        else
        {
            data.analyzed_join.addJoinMixedCondition(ast);
        }
    }
    else
    {
        auto from_tables = getTableForIdentifiers(ast, data);
        if (from_tables.empty() || from_tables.size() == 1)
        {
            auto from_table = from_tables.empty() ? JoinIdentifierPos::Right : *from_tables.begin();
            data.analyzed_join.addJoinCondition(ast, isLeftIdentifier(from_table));
        }
        else
        {
            data.analyzed_join.addJoinMixedCondition(ast);
        }
    }
}

void CollectJoinOnKeysMatcher::visit(const ASTLiteral & /*ident*/, const ASTPtr & ast, Data & data)
{
    data.analyzed_join.addJoinCondition(ast, false);
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
std::set<JoinIdentifierPos> CollectJoinOnKeysMatcher::getTableForIdentifiers(const ASTPtr & ast, const Data & data)
{
    std::vector<const ASTIdentifier *> identifiers;
    getIdentifiers(ast, identifiers);
    if (identifiers.empty())
    {
        return {};
    }

    std::set<JoinIdentifierPos> table_numbers;

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
        if (membership == JoinIdentifierPos::Left || membership == JoinIdentifierPos::Right)
        {
            table_numbers.insert(membership);
        }

    }

    return table_numbers;
}

}
