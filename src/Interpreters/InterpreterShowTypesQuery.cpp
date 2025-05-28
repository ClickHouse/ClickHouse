#include <Interpreters/InterpreterShowTypesQuery.h>
#include <Parsers/ASTShowTypesQuery.h>
#include <DataTypes/UserDefinedTypeFactory.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/logger_useful.h>

namespace DB
{

BlockIO InterpreterShowTypesQuery::execute()
{
    auto current_context = getContext();
    current_context->checkAccess(AccessType::SHOW_USER_DEFINED_TYPES);

    auto & udt_factory = UserDefinedTypeFactory::instance();

    std::vector<String> type_names = udt_factory.getAllTypeNames(current_context);

    MutableColumnPtr column_ptr = ColumnString::create();
    for (const auto & name : type_names)
    {
        column_ptr->insert(name);
    }

    Block result_block({ ColumnWithTypeAndName(std::move(column_ptr), std::make_shared<DataTypeString>(), "name") });

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(result_block));
    
    return res;
}

void registerInterpreterShowTypesQuery(InterpreterFactory & factory)
{
    auto create_fn = [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowTypesQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowTypesQuery", create_fn);
}

}
