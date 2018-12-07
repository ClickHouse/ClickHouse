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


class RequiredSourceColumnsMatcher
{
public:
    struct Data
    {
        const NameSet & available_columns;
        NameSet & required_source_columns;
        NameSet & ignored_names;
        const NameSet & available_joined_columns;
        NameSet & required_joined_columns;
    };

    static constexpr const char * label = "RequiredSourceColumns";

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child)
    {
        /// We will not go to the ARRAY JOIN section, because we need to look at the names of non-ARRAY-JOIN columns.
        /// There, `collectUsedColumns` will send us separately.
        if (typeid_cast<ASTSelectQuery *>(child.get()) ||
            typeid_cast<ASTArrayJoin *>(child.get()) ||
            typeid_cast<ASTTableExpression *>(child.get()) ||
            typeid_cast<ASTTableJoin *>(child.get()))
            return false;

        /// Processed. Do not need children.
        if (typeid_cast<ASTIdentifier *>(node.get()))
            return false;

        if (auto * f = typeid_cast<ASTFunction *>(node.get()))
        {
            /// A special function `indexHint`. Everything that is inside it is not calculated
            /// (and is used only for index analysis, see KeyCondition).
            if (f->name == "indexHint")
                return false;
        }

        return true;
    }

    /** Find all the identifiers in the query.
      * We will use depth first search in AST.
      * In this case
      * - for lambda functions we will not take formal parameters;
      * - do not go into subqueries (they have their own identifiers);
      * - there is some exception for the ARRAY JOIN clause (it has a slightly different identifiers);
      * - we put identifiers available from JOIN in required_joined_columns.
      */
    static std::vector<ASTPtr *> visit(ASTPtr & ast, Data & data)
    {
        if (auto * t = typeid_cast<ASTIdentifier *>(ast.get()))
            visit(*t, ast, data);
        if (auto * t = typeid_cast<ASTFunction *>(ast.get()))
            visit(*t, ast, data);
        return {};
    }

private:
    static void visit(const ASTIdentifier & node, const ASTPtr &, Data & data)
    {
        if (node.general()
            && !data.ignored_names.count(node.name)
            && !data.ignored_names.count(Nested::extractTableName(node.name)))
        {
            /// Read column from left table if has.
            if (!data.available_joined_columns.count(node.name) || data.available_columns.count(node.name))
                data.required_source_columns.insert(node.name);
            else
                data.required_joined_columns.insert(node.name);
        }
    }

    static void visit(const ASTFunction & node, const ASTPtr &, Data & data)
    {
        NameSet & ignored_names = data.ignored_names;

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

            visit(node.arguments->children[1], data);

            for (size_t i = 0; i < added_ignored.size(); ++i)
                ignored_names.erase(added_ignored[i]);
        }
    }
};

/** Get a set of necessary columns to read from the table.
  * In this case, the columns specified in ignored_names are considered unnecessary. And the ignored_names parameter can be modified.
  * The set of columns available_joined_columns are the columns available from JOIN, they are not needed for reading from the main table.
  * Put in required_joined_columns the set of columns available from JOIN and needed.
  */
using RequiredSourceColumnsVisitor = InDepthNodeVisitor<RequiredSourceColumnsMatcher, true>;

}
