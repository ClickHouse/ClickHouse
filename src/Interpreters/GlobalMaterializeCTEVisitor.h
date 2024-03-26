#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/MaterializedTableFromCTE.h>
#include <Parsers/ASTWithElement.h>

namespace DB
{

class GlobalMaterializeCTEVisitor
{
public:
    struct Data : WithMutableContext
    {

        explicit Data(ContextMutablePtr context_) : WithMutableContext(context_)
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
