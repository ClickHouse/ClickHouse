#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/MySQL/ASTCreateQuery.h>
#include <Parsers/MySQL/ASTCreateDefines.h>
#include <Parsers/MySQL/ASTDeclareColumn.h>

namespace DB
{

using namespace MySQLParser;

namespace MySQLVisitor
{

/// Convert MySQL CREATE query to https://clickhouse.tech/docs/en/sql-reference/statements/create/
class CreateQueryMatcher
{
public:
    using Visitor = InDepthNodeVisitor<CreateQueryMatcher, false>;

    struct Data
    {
        /// SETTINGS
        WriteBuffer & out;
        std::string declare_columns;
    };

    static void visit(ASTPtr & ast, Data & data);

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return false; }
private:
    static void visit(ASTCreateQuery & create, ASTPtr & ast, Data &);

    static void visitColumns(ASTCreateDefines & create_defines, ASTPtr & ast, Data & data);

    static void visitColumns(const ASTDeclareColumn & declare_column, ASTPtr & ast, Data & data);

//    static void visitColumns(const ASTFunction & declare_column, ASTPtr & ast, Data & data);
//    static void visit(ASTTableJoin & join, const ASTPtr & ast, Data &);

};

using CreateQueryConvertVisitor = CreateQueryMatcher::Visitor;

}

}
