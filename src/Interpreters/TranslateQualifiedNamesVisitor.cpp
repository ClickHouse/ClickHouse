#include <Poco/String.h>

#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/IdentifierSemantic.h>

#include <Common/typeid_cast.h>
#include <Common/StringUtils.h>
#include <DataTypes/DataTypeTuple.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Storages/StorageView.h>
#include <Common/re2.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNSUPPORTED_JOIN_KEYS;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_COMPILE_REGEXP;
}


bool TranslateQualifiedNamesMatcher::Data::matchColumnName(std::string_view name, const String & column_name, DataTypePtr column_type)
{
    if (name.size() < column_name.size())
        return false;

    if (!name.starts_with(column_name))
        return false;

    if (name.size() == column_name.size())
        return true;

    /// In case the type is named tuple, check the name recursively.
    if (const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(column_type.get()))
    {
        if (type_tuple->haveExplicitNames() && name.at(column_name.size()) == '.')
        {
            const Strings & names = type_tuple->getElementNames();
            const DataTypes & element_types = type_tuple->getElements();
            std::string_view sub_name = name.substr(column_name.size() + 1);
            for (size_t i = 0; i < names.size(); ++i)
            {
                if (matchColumnName(sub_name, names[i], element_types[i]))
                {
                    return true;
                }
            }
        }
    }

    return false;
}
bool TranslateQualifiedNamesMatcher::Data::unknownColumn(size_t table_pos, const ASTIdentifier & identifier) const
{
    const auto & table = tables[table_pos].table;
    const auto & columns = tables[table_pos].columns;

    // Remove database and table name from the identifier'name
    auto full_name = IdentifierSemantic::extractNestedName(identifier, table);

    for (const auto & column : columns)
    {
        if (matchColumnName(full_name, column.name, column.type))
            return false;
    }
    const auto & hidden_columns = tables[table_pos].hidden_columns;
    for (const auto & column : hidden_columns)
    {
        if (matchColumnName(full_name, column.name, column.type))
            return false;
    }
    return !columns.empty();
}

bool TranslateQualifiedNamesMatcher::needChildVisit(ASTPtr & node, const ASTPtr & child)
{
    /// Do not go to FROM, JOIN, subqueries.
    if (child->as<ASTTableExpression>() || child->as<ASTSelectWithUnionQuery>())
        return false;

    /// Processed nodes. Do not go into children.
    if (node->as<ASTQualifiedAsterisk>() || node->as<ASTTableJoin>())
        return false;

    /// ASTSelectQuery + others
    return true;
}

void TranslateQualifiedNamesMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = ast->as<ASTIdentifier>())
        visit(*t, ast, data);
    if (auto * t = ast->as<ASTTableJoin>())
        visit(*t, ast, data);
    if (auto * t = ast->as<ASTSelectQuery>())
        visit(*t, ast, data);
    if (auto * node = ast->as<ASTExpressionList>())
        visit(*node, ast, data);
    if (auto * node = ast->as<ASTFunction>())
        visit(*node, ast, data);
}

void TranslateQualifiedNamesMatcher::visit(ASTIdentifier & identifier, ASTPtr &, Data & data)
{
    if (IdentifierSemantic::getColumnName(identifier))
    {
        String short_name = identifier.shortName();
        bool allow_ambiguous = data.join_using_columns.contains(short_name);
        if (auto best_pos = IdentifierSemantic::chooseTable(identifier, data.tables, allow_ambiguous))
        {
            size_t table_pos = *best_pos;
            if (data.unknownColumn(table_pos, identifier))
            {
                String table_name = data.tables[table_pos].table.getQualifiedNamePrefix(false);
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "There's no column '{}' in table '{}'", identifier.name(), table_name);
            }

            IdentifierSemantic::setMembership(identifier, table_pos);

            /// In case if column from the joined table are in source columns, change it's name to qualified.
            /// Also always leave unusual identifiers qualified.
            const auto & table = data.tables[table_pos].table;
            if (table_pos && (data.hasColumn(short_name) || !isValidIdentifierBegin(short_name.at(0))))
                IdentifierSemantic::setColumnLongName(identifier, table);
            else
                IdentifierSemantic::setColumnShortName(identifier, table);
        }
    }
}

/// As special case, treat count(*) as count(), not as count(list of all columns).
void TranslateQualifiedNamesMatcher::visit(ASTFunction & node, const ASTPtr &, Data &)
{
    ASTPtr & func_arguments = node.arguments;

    if (!func_arguments) return;

    String func_name_lowercase = Poco::toLower(node.name);
    if ((func_name_lowercase == "count" || func_name_lowercase == "countstate") &&
        func_arguments->children.size() == 1 &&
        func_arguments->children[0]->as<ASTAsterisk>())
        func_arguments->children.clear();
}

void TranslateQualifiedNamesMatcher::visit(const ASTQualifiedAsterisk & node, const ASTPtr &, Data & data)
{
    if (!node.qualifier)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Qualified asterisk must have a qualifier");

    /// @note it could contain table alias as table name.
    DatabaseAndTableWithAlias db_and_table(node.qualifier);

    for (const auto & known_table : data.tables)
        if (db_and_table.satisfies(known_table.table, true))
            return;

    throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown qualified identifier: {}", node.qualifier->getAliasOrColumnName());
}

void TranslateQualifiedNamesMatcher::visit(ASTTableJoin & join, const ASTPtr & , Data & data)
{
    if (join.using_expression_list)
        Visitor(data).visit(join.using_expression_list);
    else if (join.on_expression)
        Visitor(data).visit(join.on_expression);
}

void TranslateQualifiedNamesMatcher::visit(ASTSelectQuery & select, const ASTPtr & , Data & data)
{
    if (const auto * join = select.join())
        extractJoinUsingColumns(join->table_join, data);

    /// If the WHERE clause or HAVING consists of a single qualified column, the reference must be translated not only in children,
    /// but also in where_expression and having_expression.
    if (select.prewhere())
        Visitor(data).visit(select.refPrewhere());
    if (select.where())
        Visitor(data).visit(select.refWhere());
    if (select.having())
        Visitor(data).visit(select.refHaving());
}

static void addIdentifier(ASTs & nodes, const DatabaseAndTableWithAlias & table, const String & column_name)
{
    std::vector<String> parts = {column_name};

    String table_name = table.getQualifiedNamePrefix(false);
    if (!table_name.empty()) parts.insert(parts.begin(), table_name);

    nodes.emplace_back(std::make_shared<ASTIdentifier>(std::move(parts)));
}

/// Replace *, alias.*, database.table.* with a list of columns.
void TranslateQualifiedNamesMatcher::visit(ASTExpressionList & node, const ASTPtr &, Data & data)
{
    const auto & tables_with_columns = data.tables;

    ASTs old_children;
    if (data.processAsterisks())
    {
        bool has_asterisk = false;
        for (const auto & child : node.children)
        {
            if (child->as<ASTAsterisk>() || child->as<ASTColumnsListMatcher>() || child->as<ASTColumnsRegexpMatcher>())
            {
                if (tables_with_columns.empty())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "An asterisk cannot be replaced with empty columns.");
                has_asterisk = true;
            }
            else if (const auto * qa = child->as<ASTQualifiedAsterisk>())
            {
                visit(*qa, child, data); /// check if it's OK before rewrite
                has_asterisk = true;
            }
        }

        if (has_asterisk)
        {
            old_children.swap(node.children);
            node.children.reserve(old_children.size());
        }
    }

    for (const auto & child : old_children)
    {
        ASTs columns;
        if (const auto * asterisk = child->as<ASTAsterisk>())
        {
            bool first_table = true;
            for (const auto & table : tables_with_columns)
            {
                for (const auto * cols : {&table.columns, &table.alias_columns, &table.materialized_columns})
                {
                    for (const auto & column : *cols)
                    {
                        if (first_table || !data.join_using_columns.contains(column.name))
                        {
                            std::string column_name = column.name;
                            addIdentifier(columns, table.table, column_name);
                        }
                    }
                }
                first_table = false;
            }

            if (asterisk->transformers)
            {
                for (const auto & transformer : asterisk->transformers->children)
                    IASTColumnsTransformer::transform(transformer, columns);
            }
        }
        else if (auto * asterisk_column_list = child->as<ASTColumnsListMatcher>())
        {
            for (const auto & ident : asterisk_column_list->column_list->children)
                columns.emplace_back(ident->clone());

            if (asterisk_column_list->transformers)
            {
                for (const auto & transformer : asterisk_column_list->transformers->children)
                    IASTColumnsTransformer::transform(transformer, columns);
            }
        }
        else if (const auto * asterisk_regexp_pattern = child->as<ASTColumnsRegexpMatcher>())
        {
            String pattern = asterisk_regexp_pattern->getPattern();
            re2::RE2 regexp(pattern, re2::RE2::Quiet);
            if (!regexp.ok())
                throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                    "COLUMNS pattern {} cannot be compiled: {}", pattern, regexp.error());

            bool first_table = true;
            for (const auto & table : tables_with_columns)
            {
                for (const auto & column : table.columns)
                {
                    if (re2::RE2::PartialMatch(column.name, regexp)
                        && (first_table || !data.join_using_columns.contains(column.name)))
                    {
                        addIdentifier(columns, table.table, column.name);
                    }
                }
                first_table = false;
            }

            if (asterisk_regexp_pattern->transformers)
            {
                for (const auto & transformer : asterisk_regexp_pattern->transformers->children)
                    IASTColumnsTransformer::transform(transformer, columns);
            }
        }
        else if (const auto * qualified_asterisk = child->as<ASTQualifiedAsterisk>())
        {
            DatabaseAndTableWithAlias ident_db_and_name(qualified_asterisk->qualifier);

            for (const auto & table : tables_with_columns)
            {
                if (ident_db_and_name.satisfies(table.table, true))
                {
                    for (const auto & column : table.columns)
                        addIdentifier(columns, table.table, column.name);
                    break;
                }
            }

            if (qualified_asterisk->transformers)
            {
                for (const auto & transformer : qualified_asterisk->transformers->children)
                    IASTColumnsTransformer::transform(transformer, columns);
            }
        }
        else
            columns.emplace_back(child);

        node.children.insert(
            node.children.end(),
            std::make_move_iterator(columns.begin()),
            std::make_move_iterator(columns.end()));
    }
}

/// 'select * from a join b using id' should result one 'id' column
void TranslateQualifiedNamesMatcher::extractJoinUsingColumns(ASTPtr ast, Data & data)
{
    const auto & table_join = ast->as<ASTTableJoin &>();

    if (table_join.using_expression_list)
    {
        const auto & keys = table_join.using_expression_list->as<ASTExpressionList &>();
        for (const auto & key : keys.children)
            if (auto opt_column = tryGetIdentifierName(key))
                data.join_using_columns.insert(*opt_column);
            else if (key->as<ASTLiteral>())
                data.join_using_columns.insert(key->getColumnName());
            else
            {
                String alias = key->tryGetAlias();
                if (alias.empty())
                    throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Wrong key in USING. Expected identifier or alias, got: {}",
                                    key->getID());
                data.join_using_columns.insert(alias);
            }
    }
}


void RestoreQualifiedNamesMatcher::Data::changeTable(ASTIdentifier & identifier) const
{
    auto match = IdentifierSemantic::canReferColumnToTable(identifier, distributed_table);
    switch (match)
    {
        case IdentifierSemantic::ColumnMatch::AliasedTableName:
        case IdentifierSemantic::ColumnMatch::TableName:
        case IdentifierSemantic::ColumnMatch::DBAndTable:
            IdentifierSemantic::setColumnLongName(identifier, remote_table);
            break;
        default:
            break;
    }
}

bool RestoreQualifiedNamesMatcher::needChildVisit(ASTPtr &, const ASTPtr & child)
{
    /// Do not go into subqueries
    if (child->as<ASTSelectWithUnionQuery>())
        return false; // NOLINT
    return true;
}

void RestoreQualifiedNamesMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = ast->as<ASTIdentifier>())
        visit(*t, ast, data);
}

void RestoreQualifiedNamesMatcher::visit(ASTIdentifier & identifier, ASTPtr &, Data & data)
{
    if (IdentifierSemantic::getColumnName(identifier))
    {
        if (IdentifierSemantic::getMembership(identifier))
        {
            identifier.restoreTable();  // TODO(ilezhankin): should restore qualified name here - why exactly here?
            data.changeTable(identifier);
        }
    }
}

}
