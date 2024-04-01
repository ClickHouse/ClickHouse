#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/MaterializedTableFromCTE.h>
#include <Parsers/ASTWithElement.h>
#include <Interpreters/Context.h>

namespace DB
{

class GlobalMaterializeCTEVisitor
{
public:
    struct Data : WithMutableContext
    {

        explicit Data(ContextMutablePtr context_, FutureTablesFromCTE & future_tables_)
        : WithMutableContext(context_)
        , future_tables(future_tables_)
        {
        }
        FutureTablesFromCTE & future_tables;
        void addExternalStorage(const ASTWithElement & cte_expr);
    };

    explicit GlobalMaterializeCTEVisitor(Data & data_) : data(data_) {}

    /// Not visiting subqueries
    void visit(ASTPtr & ast);

private:
    Data & data;
};

}
