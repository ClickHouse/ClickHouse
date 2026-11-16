#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTFunction;

/// Removes unneeded injective functions inside `uniq*()`.
class RemoveInjectiveFunctionsMatcher
{
public:
    struct Data : public WithContext
    {
        Data(ContextPtr context_, const NamesAndTypesList & source_columns_, NameSet array_join_result_names_)
            : WithContext(context_), source_columns(source_columns_), array_join_result_names(std::move(array_join_result_names_)) {}

        const NamesAndTypesList & source_columns;
        /// See `getArrayJoinResultNames`.
        NameSet array_join_result_names;
    };

    static void visit(ASTPtr & ast, const Data & data);
    static void visit(ASTFunction &, ASTPtr & ast, const Data & data);

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
};

using RemoveInjectiveFunctionsVisitor = InDepthNodeVisitor<RemoveInjectiveFunctionsMatcher, true>;

}
