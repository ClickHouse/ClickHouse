#pragma once

#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ASTAlterQuery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/misc.h>

namespace DB
{

/// Visitors consist of functions with unified interface 'void visit(Casted & x, ASTPtr & y)', there x is y, successfully casted to Casted.
/// Both types and function could have const specifiers. The second argument is used by visitor to replaces AST node (y) if needed.

/// Visits AST nodes, add default database to tables if not set. There's different logic for DDLs and selects.
class AddDefaultDatabaseVisitor
{
public:
    explicit AddDefaultDatabaseVisitor(
        ContextPtr context_,
        const String & database_name_,
        bool only_replace_current_database_function_ = false)
        : context(context_)
        , database_name(database_name_)
        , only_replace_current_database_function(only_replace_current_database_function_)
    {}

    void visitDDL(ASTPtr & ast) const
    {
        visitDDLChildren(ast);

        if (!tryVisitDynamicCast<ASTAlterQuery>(ast) &&
            !tryVisitDynamicCast<ASTQueryWithTableAndOutput>(ast) &&
            !tryVisitDynamicCast<ASTRenameQuery>(ast) &&
            !tryVisitDynamicCast<ASTFunction>(ast))
        {}
    }

    void visit(ASTPtr & ast) const
    {
        if (!tryVisit<ASTSelectQuery>(ast) &&
            !tryVisit<ASTSelectWithUnionQuery>(ast) &&
            !tryVisit<ASTFunction>(ast))
            visitChildren(*ast);
    }

    void visit(ASTSelectQuery & select) const
    {
        ASTPtr unused;
        visit(select, unused);
    }

    void visit(ASTSelectWithUnionQuery & select) const
    {
        ASTPtr unused;
        visit(select, unused);
    }

    void visit(ASTColumns & columns) const
    {
        for (auto & child : columns.children)
            visit(child);
    }

private:

    ContextPtr context;

    const String database_name;

    bool only_replace_current_database_function = false;

    void visit(ASTSelectWithUnionQuery & select, ASTPtr &) const
    {
        for (auto & child : select.list_of_selects->children)
            tryVisit<ASTSelectQuery>(child);
    }

    void visit(ASTSelectQuery & select, ASTPtr &) const
    {
        if (select.tables())
            tryVisit<ASTTablesInSelectQuery>(select.refTables());

        visitChildren(select);
    }

    void visit(ASTTablesInSelectQuery & tables, ASTPtr &) const
    {
        for (auto & child : tables.children)
            tryVisit<ASTTablesInSelectQueryElement>(child);
    }

    void visit(ASTTablesInSelectQueryElement & tables_element, ASTPtr &) const
    {
        if (tables_element.table_expression)
            tryVisit<ASTTableExpression>(tables_element.table_expression);
    }

    void visit(ASTTableExpression & table_expression, ASTPtr &) const
    {
        if (table_expression.database_and_table_name)
            tryVisit<ASTTableIdentifier>(table_expression.database_and_table_name);
        else if (table_expression.subquery)
            tryVisit<ASTSubquery>(table_expression.subquery);
    }

    void visit(const ASTTableIdentifier & identifier, ASTPtr & ast) const
    {
        if (!identifier.compound())
        {
            auto qualified_identifier = std::make_shared<ASTTableIdentifier>(database_name, identifier.name());
            if (!identifier.alias.empty())
                qualified_identifier->setAlias(identifier.alias);
            ast = qualified_identifier;
        }
    }

    void visit(ASTSubquery & subquery, ASTPtr &) const
    {
        tryVisit<ASTSelectWithUnionQuery>(subquery.children[0]);
    }

    void visit(ASTFunction & function, ASTPtr &) const
    {
        bool is_operator_in = functionIsInOrGlobalInOperator(function.name);
        bool is_dict_get = functionIsDictGet(function.name);

        for (auto & child : function.children)
        {
            if (child.get() == function.arguments.get())
            {
                for (size_t i = 0; i < child->children.size(); ++i)
                {
                    if (is_dict_get && i == 0)
                    {
                        if (auto * identifier = child->children[i]->as<ASTIdentifier>())
                        {
                            /// Identifier already qualified
                            if (identifier->compound())
                                continue;

                            auto qualified_dictionary_name = context->getExternalDictionariesLoader().qualifyDictionaryNameWithDatabase(identifier->name(), context);
                            child->children[i] = std::make_shared<ASTIdentifier>(qualified_dictionary_name.getParts());
                        }
                        else if (auto * literal = child->children[i]->as<ASTLiteral>())
                        {
                            auto & literal_value = literal->value;

                            if (literal_value.getType() != Field::Types::String)
                                continue;

                            auto dictionary_name = literal_value.get<String>();
                            auto qualified_dictionary_name = context->getExternalDictionariesLoader().qualifyDictionaryNameWithDatabase(dictionary_name, context);
                            literal_value = qualified_dictionary_name.getFullName();
                        }
                    }
                    else if (is_operator_in && i == 1)
                    {
                        /// XXX: for some unknown reason this place assumes that argument can't be an alias,
                        ///      like in the similar code in `MarkTableIdentifierVisitor`.
                        if (auto * identifier = child->children[i]->as<ASTIdentifier>())
                        {
                            /// If identifier is broken then we can do nothing and get an exception
                            auto maybe_table_identifier = identifier->createTable();
                            if (maybe_table_identifier)
                                child->children[i] = maybe_table_identifier;
                        }

                        /// Second argument of the "in" function (or similar) may be a table name or a subselect.
                        /// Rewrite the table name or descend into subselect.
                        if (!tryVisit<ASTTableIdentifier>(child->children[i]))
                            visit(child->children[i]);
                    }
                    else
                    {
                        visit(child->children[i]);
                    }
                }
            }
            else
            {
                visit(child);
            }
        }
    }

    void visitChildren(IAST & ast) const
    {
        for (auto & child : ast.children)
            visit(child);
    }

    template <typename T>
    bool tryVisit(ASTPtr & ast) const
    {
        if (T * t = typeid_cast<T *>(ast.get()))
        {
            visit(*t, ast);
            return true;
        }
        return false;
    }


    void visitDDL(ASTQueryWithTableAndOutput & node, ASTPtr &) const
    {
        if (only_replace_current_database_function)
            return;

        if (node.database.empty())
            node.database = database_name;
    }

    void visitDDL(ASTRenameQuery & node, ASTPtr &) const
    {
        if (only_replace_current_database_function)
            return;

        for (ASTRenameQuery::Element & elem : node.elements)
        {
            if (elem.from.database.empty())
                elem.from.database = database_name;
            if (elem.to.database.empty())
                elem.to.database = database_name;
        }
    }

    void visitDDL(ASTAlterQuery & node, ASTPtr &) const
    {
        if (only_replace_current_database_function)
            return;

        if (node.database.empty())
            node.database = database_name;

        for (const auto & child : node.command_list->children)
        {
            auto * command_ast = child->as<ASTAlterCommand>();
            if (command_ast->from_database.empty())
                command_ast->from_database = database_name;
            if (command_ast->to_database.empty())
                command_ast->to_database = database_name;
        }
    }

    void visitDDL(ASTFunction & function, ASTPtr & node) const
    {
        if (function.name == "currentDatabase")
        {
            node = std::make_shared<ASTLiteral>(database_name);
            return;
        }
    }

    void visitDDLChildren(ASTPtr & ast) const
    {
        for (auto & child : ast->children)
            visitDDL(child);
    }

    template <typename T>
    bool tryVisitDynamicCast(ASTPtr & ast) const
    {
        if (T * t = dynamic_cast<T *>(ast.get()))
        {
            visitDDL(*t, ast);
            return true;
        }
        return false;
    }
};

}
