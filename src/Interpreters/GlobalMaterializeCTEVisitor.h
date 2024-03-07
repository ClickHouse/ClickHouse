#pragma once

#include <memory>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Databases/IDatabase.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/interpretSubquery.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/TableJoin.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include "Common/tests/gtest_global_context.h"
#include <Common/typeid_cast.h>
#include "Interpreters/MaterializedTableFromCTE.h"
#include "Parsers/ASTExplainQuery.h"
#include <Parsers/ASTWithElement.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int WRONG_GLOBAL_SUBQUERY;
    extern const int LOGICAL_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
}

class GlobalMaterializeCTEVisitor
{
public:
    struct Data : WithMutableContext
    {
        size_t subquery_depth;
        FutureTablesFromCTE & future_tables;

        Data(
            ContextMutablePtr context_,
            FutureTablesFromCTE & future_tables_)
            : WithMutableContext(context_)
            , future_tables(future_tables_)
        {
        }

        void addExternalStorage(ASTWithElement & cte_expr, const Names & required_columns);
    };

    explicit GlobalMaterializeCTEVisitor(Data & data_) : data(data_) {}

    /// Not visiting subqueries
    void visit(ASTPtr & ast);

private:
    Data & data;
};

}
