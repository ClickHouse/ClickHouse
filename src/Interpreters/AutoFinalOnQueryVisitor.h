#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class AutoFinalOnQuery
{
public:
    struct Data
    {
        StoragePtr storage;
        ContextPtr context;
    };

    static bool needChildVisit(ASTPtr &, const ASTPtr &);
    static void visit(ASTPtr & query, Data & data);

private:
    static void visit(ASTSelectQuery & select, StoragePtr storage, ContextPtr context);
    static bool autoFinalOnQuery(ASTSelectQuery & select_query, StoragePtr storage, ContextPtr context);

};

using AutoFinalOnQueryVisitor = InDepthNodeVisitor<AutoFinalOnQuery, true>;

}
