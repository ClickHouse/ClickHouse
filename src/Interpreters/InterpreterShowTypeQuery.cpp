#include <Interpreters/InterpreterShowTypeQuery.h>

#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/UserDefinedTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTShowTypeQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

BlockIO InterpreterShowTypeQuery::execute()
{
    const auto & show_query = query_ptr->as<const ASTShowTypeQuery &>();

    auto current_context = getContext();
    current_context->checkAccess(AccessType::SHOW_USER_DEFINED_TYPES);

    /// Throws `UNKNOWN_TYPE` for a type that does not exist.
    auto create_query = UserDefinedTypeFactory::instance().get(show_query.type_name);
    const auto & create = create_query->as<const ASTCreateTypeQuery &>();

    auto name_column = ColumnString::create();
    auto base_type_column = ColumnString::create();
    auto type_parameters_column = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    auto create_query_column = ColumnString::create();

    name_column->insert(show_query.type_name);
    base_type_column->insert(create.base_type->formatWithSecretsOneLine());
    if (create.type_parameters)
        type_parameters_column->insert(create.type_parameters->formatWithSecretsOneLine());
    else
        type_parameters_column->insertDefault();
    create_query_column->insert(create_query->formatWithSecretsOneLine());

    Block result_block
    {
        ColumnWithTypeAndName(std::move(name_column), std::make_shared<DataTypeString>(), "name"),
        ColumnWithTypeAndName(std::move(base_type_column), std::make_shared<DataTypeString>(), "base_type"),
        ColumnWithTypeAndName(std::move(type_parameters_column), std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "type_parameters"),
        ColumnWithTypeAndName(std::move(create_query_column), std::make_shared<DataTypeString>(), "create_query"),
    };

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(result_block))));
    return res;
}

void registerInterpreterShowTypeQuery(InterpreterFactory & factory)
{
    auto create_fn = [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowTypeQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowTypeQuery", create_fn);
}

}
