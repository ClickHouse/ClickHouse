#pragma once

#include <Core/Names.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <DataTypes/NestedUtils.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Visitors consist of functions with unified interface 'void visit(Casted & x, ASTPtr & y)', there x is y, successfully casted to Casted.
/// Both types and fuction could have const specifiers. The second argument is used by visitor to replaces AST node (y) if needed.

/** Get a set of necessary columns to read from the table.
  * In this case, the columns specified in ignored_names are considered unnecessary. And the ignored_names parameter can be modified.
  * The set of columns available_joined_columns are the columns available from JOIN, they are not needed for reading from the main table.
  * Put in required_joined_columns the set of columns available from JOIN and needed.
  */
class RequiredSourceColumnsVisitor
{
public:
    RequiredSourceColumnsVisitor(const NameSet & available_columns_, NameSet & required_source_columns_, NameSet & ignored_names_,
                                 const NameSet & available_joined_columns_, NameSet & required_joined_columns_)
    :   available_columns(available_columns_),
        required_source_columns(required_source_columns_),
        ignored_names(ignored_names_),
        available_joined_columns(available_joined_columns_),
        required_joined_columns(required_joined_columns_)
    {}

    /** Find all the identifiers in the query.
      * We will use depth first search in AST.
      * In this case
      * - for lambda functions we will not take formal parameters;
      * - do not go into subqueries (they have their own identifiers);
      * - there is some exception for the ARRAY JOIN clause (it has a slightly different identifiers);
      * - we put identifiers available from JOIN in required_joined_columns.
      */
    void visit(const ASTPtr & ast) const
    {
        if (!tryVisit<ASTIdentifier>(ast) &&
            !tryVisit<ASTFunction>(ast))
            visitChildren(ast);
    }

private:
    const NameSet & available_columns;
    NameSet & required_source_columns;
    NameSet & ignored_names;
    const NameSet & available_joined_columns;
    NameSet & required_joined_columns;

    void visit(const ASTIdentifier & node, const ASTPtr &) const
    {
        if (node.general()
            && !ignored_names.count(node.name)
            && !ignored_names.count(Nested::extractTableName(node.name)))
        {
            if (!available_joined_columns.count(node.name)
                || available_columns.count(node.name)) /// Read column from left table if has.
                required_source_columns.insert(node.name);
            else
                required_joined_columns.insert(node.name);
        }
    }

    void visit(const ASTFunction & node, const ASTPtr & ast) const
    {
        if (node.name == "lambda")
        {
            if (node.arguments->children.size() != 2)
                throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            ASTFunction * lambda_args_tuple = typeid_cast<ASTFunction *>(node.arguments->children.at(0).get());

            if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
                throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);

            /// You do not need to add formal parameters of the lambda expression in required_source_columns.
            Names added_ignored;
            for (auto & child : lambda_args_tuple->arguments->children)
            {
                ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(child.get());
                if (!identifier)
                    throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);

                String & name = identifier->name;
                if (!ignored_names.count(name))
                {
                    ignored_names.insert(name);
                    added_ignored.push_back(name);
                }
            }

            visit(node.arguments->children.at(1));

            for (size_t i = 0; i < added_ignored.size(); ++i)
                ignored_names.erase(added_ignored[i]);

            return;
        }

        /// A special function `indexHint`. Everything that is inside it is not calculated
        /// (and is used only for index analysis, see KeyCondition).
        if (node.name == "indexHint")
            return;

        visitChildren(ast);
    }

    void visitChildren(const ASTPtr & ast) const
    {
        for (auto & child : ast->children)
        {
            /** We will not go to the ARRAY JOIN section, because we need to look at the names of non-ARRAY-JOIN columns.
            * There, `collectUsedColumns` will send us separately.
            */
            if (!typeid_cast<const ASTSelectQuery *>(child.get())
                && !typeid_cast<const ASTArrayJoin *>(child.get())
                && !typeid_cast<const ASTTableExpression *>(child.get())
                && !typeid_cast<const ASTTableJoin *>(child.get()))
                visit(child);
        }
    }

    template <typename T>
    bool tryVisit(const ASTPtr & ast) const
    {
        if (const T * t = typeid_cast<const T *>(ast.get()))
        {
            visit(*t, ast);
            return true;
        }
        return false;
    }
};

}
