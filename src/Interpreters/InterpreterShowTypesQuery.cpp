#include <Interpreters/InterpreterShowTypesQuery.h>

#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/UserDefinedTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTShowTypesQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

BlockIO InterpreterShowTypesQuery::execute()
{
    auto current_context = getContext();
    current_context->checkAccess(AccessType::SHOW_USER_DEFINED_TYPES);

    auto column = ColumnString::create();
    for (const auto & name : UserDefinedTypeFactory::instance().getAllRegisteredNames())
        column->insert(name);

    Block result_block({ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "name")});

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(result_block))));
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
