#pragma once

#include <Core/QualifiedTableName.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Databases/DDLLoadingDependencyVisitor.h>


namespace DB
{

/// Evaluates constants in DDL query.
class NormalizeAndEvaluateConstants : public DDLMatcherBase
{
public:
    struct Data
    {
        ContextPtr create_query_context;
    };

    using Visitor = ConstInDepthNodeVisitor<NormalizeAndEvaluateConstants, true>;

    static void visit(const ASTPtr & ast, Data & data);

private:
    static void visit(const ASTFunction & function, Data & data);
    static void visit(const ASTFunctionWithKeyValueArguments & dict_source, Data & data);
};

using NormalizeAndEvaluateConstantsVisitor = NormalizeAndEvaluateConstants::Visitor;

}
