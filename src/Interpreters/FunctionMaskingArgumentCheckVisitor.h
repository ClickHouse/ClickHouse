#pragma once


#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

/// Checks from bottom to top if a function's alias shadows the name
/// of one of it's arguments, e.g.
/// SELECT toString(dummy) as dummy FROM system.one GROUP BY dummy;
class FunctionMaskingArgumentCheckMatcher
{
public:
    struct Data
    {
        const String& alias;
        bool is_rejected = false;
        void reject() { is_rejected = true; }
    };

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (data.is_rejected)
            return;
        if (const auto & identifier = ast->as<ASTIdentifier>())
            visit(*identifier, data);
    }

    static void visit(const ASTIdentifier & ast, Data & data)
    {
        if (ast.getAliasOrColumnName() == data.alias)
            data.reject();
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using FunctionMaskingArgumentCheckVisitor = ConstInDepthNodeVisitor<FunctionMaskingArgumentCheckMatcher, false>;

}
