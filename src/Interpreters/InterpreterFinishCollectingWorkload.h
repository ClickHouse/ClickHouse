#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTStartCollectingWorkloadQuery.h>
#include <Core/Field.h>
#include <Interpreters/IndexAdvisor/QueryInfo.h>
#include <Interpreters/IndexAdvisor/TableManager.h>
#include <Interpreters/IndexAdvisor/IndexAdvisor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/BlockIO.h>

namespace DB
{

class InterpreterFinishCollectingWorkload : public IInterpreter
{
public:
    InterpreterFinishCollectingWorkload(const ASTPtr &, ContextMutablePtr context_)
    : context(context_) {}

    BlockIO execute() override
    {
        executeQuery("SET collect_workload = 0", context, QueryFlags{ .internal = true });

        QueryInfo query_info("/tmp/workload_collection.txt", context);
        // for (const auto & query : query_info.getWorkload())
        // {
        //     LOG_DEBUG(getLogger("InterpreterFinishCollectingWorkload"), "Query: {}", query);
        // }
        //  for (const auto & table : query_info.getTables())
        // {
        //     for (const auto & col : query_info.getColumns(table))
        //     {
        //         LOG_DEBUG(getLogger("InterpreterFinishCollectingWorkload"), "table: {}, columns {}", table, col);
        //     }
        // }
       
        TableManager table_manager(query_info, context);
        // LOG_INFO(getLogger("InterpreterFinishCollectingWorkload"), "TableManager start");
        // for (const auto & table : table_manager.getTables())
        // {
        //     for (const auto & col : table_manager.getColumns(table))
        //     {
        //         LOG_DEBUG(getLogger("InterpreterFinishCollectingWorkload"), "table: {}, columns {}", table, col);
        //     }
        // }

        IndexAdvisor index_advisor(table_manager, context);
        // LOG_INFO(getLogger("InterpreterFinishCollectingWorkload"), "IndexAdvisor start");
        auto res = index_advisor.getBestPKColumns();
        // LOG_INFO(getLogger("InterpreterFinishCollectingWorkload"), "IndexAdvisor end");
        MutableColumns columns;
        columns.emplace_back(ColumnString::create());
        columns.emplace_back(ColumnString::create());

        for (const auto & table : res)
        {
            columns[0]->insert(table.first);
            String pk_columns;
            for (const auto & col : table.second)
            {
                if (!pk_columns.empty())
                    pk_columns += ", ";
                pk_columns += col;
            }
            columns[1]->insert(pk_columns);
            LOG_INFO(getLogger("InterpreterFinishCollectingWorkload"), "table: {}, pk_columns: {}", table.first, pk_columns);
        }

        Block block;
        block.insert(ColumnWithTypeAndName{
            std::move(columns[0]),
            std::make_shared<DataTypeString>(),
            "table_name"
        });
        block.insert(ColumnWithTypeAndName{
            std::move(columns[1]),
            std::make_shared<DataTypeString>(),
            "primary_key_columns"
        });
        BlockIO block_io;
        block_io.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::move(block)));
        return block_io;
    }
protected:
    ContextMutablePtr context;
};

}
