#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

/// Visits AST node to rewrite alias columns in filter query
/// Currently works only in `KeyCondition` of select query
class ColumnAliasesMatcher
{
public:
    using Visitor = InDepthNodeVisitor<ColumnAliasesMatcher, false>;

    struct Data
    {
        const ColumnsDescription & columns;

        Data(const ColumnsDescription & columns_)
        : columns(columns_)
        {}
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
};

using ColumnAliasesVisitor = ColumnAliasesMatcher::Visitor;

}
