#include <Interpreters/InterpreterIndexAdvisorQuery.h>
#include <Interpreters/Context.h>
#include <Utils/IndexAdvisor/ASTIndexAdvisorQuery.h>
#include <Utils/IndexAdvisor/IndexAdvisor.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/Pipe.h>
#include <algorithm>
#include <Common/logger_useful.h>
#include <Interpreters/InterpreterFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

BlockIO InterpreterIndexAdvisorQuery::execute()
{
    LOG_DEBUG(&Poco::Logger::get("InterpreterIndexAdvisorQuery"), "Starting index advisor analysis");

    // Get the query to analyze
    const auto & query = query_ptr->as<const ASTIndexAdvisorQuery &>();
    if (!query.queries)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No query provided to analyze");
    }

    // Create result block
    MutableColumns columns;
    columns.emplace_back(ColumnString::create()); // table
    columns.emplace_back(ColumnString::create()); // column
    columns.emplace_back(ColumnString::create()); // index_type
    columns.emplace_back(ColumnString::create()); // reason
    columns.emplace_back(ColumnFloat64::create()); // score
    columns.emplace_back(ColumnUInt64::create()); // baseline_rows
    columns.emplace_back(ColumnUInt64::create()); // indexed_rows
    columns.emplace_back(ColumnUInt64::create()); // index_size

    // Create advisor instance
    IndexAdvisor advisor;

    // Analyze each query
    for (const auto & query_ast : query.queries->children)
    {
        LOG_DEBUG(&Poco::Logger::get("InterpreterIndexAdvisorQuery"), "Analyzing query");
        advisor.analyzeQuery(query_ast, getContext());
    }

    // Get the candidates and add them to the result
    const auto & candidates = advisor.getCandidates();
    LOG_DEBUG(&Poco::Logger::get("InterpreterIndexAdvisorQuery"), "Found {} candidates", candidates.size());

    for (const auto & candidate : candidates)
    {
        columns[0]->insert(candidate.table_name);
        columns[1]->insert(candidate.column_name);
        columns[2]->insert(candidate.index_type);
        columns[3]->insert(candidate.reason);
        columns[4]->insert(candidate.score);
        columns[5]->insert(candidate.baseline_rows);
        columns[6]->insert(candidate.indexed_rows);
        columns[7]->insert(candidate.index_size);
    }

    LOG_DEBUG(&Poco::Logger::get("InterpreterIndexAdvisorQuery"), "Total candidates found: {}", candidates.size());

    // Create the result block
    Block block;
    block.insert(ColumnWithTypeAndName(columns[0]->getPtr(), std::make_shared<DataTypeString>(), "table"));
    block.insert(ColumnWithTypeAndName(columns[1]->getPtr(), std::make_shared<DataTypeString>(), "column"));
    block.insert(ColumnWithTypeAndName(columns[2]->getPtr(), std::make_shared<DataTypeString>(), "index_type"));
    block.insert(ColumnWithTypeAndName(columns[3]->getPtr(), std::make_shared<DataTypeString>(), "reason"));
    block.insert(ColumnWithTypeAndName(columns[4]->getPtr(), std::make_shared<DataTypeFloat64>(), "score"));
    block.insert(ColumnWithTypeAndName(columns[5]->getPtr(), std::make_shared<DataTypeUInt64>(), "baseline_rows"));
    block.insert(ColumnWithTypeAndName(columns[6]->getPtr(), std::make_shared<DataTypeUInt64>(), "indexed_rows"));
    block.insert(ColumnWithTypeAndName(columns[7]->getPtr(), std::make_shared<DataTypeUInt64>(), "index_size"));

    // Create the result
    BlockIO res;
    auto source = std::make_shared<SourceFromSingleChunk>(block);
    res.pipeline = QueryPipeline(Pipe(std::move(source)));
    return res;
}

void registerInterpreterIndexAdvisorQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterIndexAdvisorQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterIndexAdvisorQuery", create_fn);
}

} 
