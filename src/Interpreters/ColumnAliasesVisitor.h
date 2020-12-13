#pragma once

#include <Core/Names.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
class IDataType;
class ASTFunction;
class ASTIdentifier;
using DataTypePtr = std::shared_ptr<const IDataType>;

/// Visits AST node to rewrite alias columns in filter query
/// Currently works only in `KeyCondition` of select query and `required_columns` in `InterpreterSelectQuery.cpp`
class ColumnAliasesMatcher
{
public:
    using Visitor = InDepthNodeVisitor<ColumnAliasesMatcher, false>;

    struct Data
    {
        const ColumnsDescription & columns;
        const NameSet & forbidden_columns;
        const Context & context;

        NameSet private_aliases;

        Data(const ColumnsDescription & columns_, const NameSet & forbidden_columns_, const Context & context_)
        : columns(columns_)
        , forbidden_columns(forbidden_columns_)
        , context(context_)
        {}
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);

private:
    static void visit(ASTIdentifier & node, ASTPtr & ast, Data & data);
    static void visit(ASTFunction & node, ASTPtr & ast, Data & data);
};

using ColumnAliasesVisitor = ColumnAliasesMatcher::Visitor;

}
