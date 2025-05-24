#include <Interpreters/InterpreterShowTypeQuery.h>
#include <Parsers/ASTShowTypeQuery.h>
#include <DataTypes/UserDefinedTypeFactory.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}

BlockIO InterpreterShowTypeQuery::execute()
{
    const auto & show_query = query_ptr->as<const ASTShowTypeQuery &>();

    auto current_context = getContext();
    current_context->checkAccess(AccessType::SHOW_USER_DEFINED_TYPES);

    auto & udt_factory = UserDefinedTypeFactory::instance();

    if (!udt_factory.isTypeRegistered(show_query.type_name, current_context))
    {
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown type {}", show_query.type_name);
    }

    auto type_info = udt_factory.getTypeInfo(show_query.type_name, current_context);

    MutableColumns result_columns;
    result_columns.emplace_back(ColumnString::create());
    result_columns.emplace_back(ColumnString::create());
    result_columns.emplace_back(ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()));
    result_columns.emplace_back(ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()));
    result_columns.emplace_back(ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()));
    result_columns.emplace_back(ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()));
    result_columns.emplace_back(ColumnString::create());

    result_columns[0]->insert(show_query.type_name);
    result_columns[1]->insert(UserDefinedTypeFactory::astToString(type_info.base_type_ast));

    if (type_info.type_parameters)
        result_columns[2]->insert(UserDefinedTypeFactory::astToString(type_info.type_parameters));
    else
        result_columns[2]->insertDefault();

    if (!type_info.input_expression.empty())
        result_columns[3]->insert(type_info.input_expression);
    else
        result_columns[3]->insertDefault();

    if (!type_info.output_expression.empty())
        result_columns[4]->insert(type_info.output_expression);
    else
        result_columns[4]->insertDefault();

    if (!type_info.default_expression.empty())
        result_columns[5]->insert(type_info.default_expression);
    else
        result_columns[5]->insertDefault();

    result_columns[6]->insert(type_info.create_query_string);

    NamesAndTypes headers = {
        {"name", std::make_shared<DataTypeString>()},
        {"base_type_ast_string", std::make_shared<DataTypeString>()},
        {"type_parameters_ast_string", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"input_expression", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"output_expression", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"default_expression", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"create_query_string", std::make_shared<DataTypeString>()}
    };

    Block result_block;
    for (size_t i = 0; i < headers.size(); ++i)
    {
        result_block.insert(ColumnWithTypeAndName(
            std::move(result_columns[i]), 
            headers[i].type, 
            headers[i].name
        ));
    }

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(result_block));
    
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
