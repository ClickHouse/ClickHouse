#include <cstddef>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <IO/CompressionMethod.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/EmptyReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>
#include "DataTypes/DataTypesNumber.h"


namespace DB
{
namespace Setting
{
    extern const SettingsBool input_format_defaults_for_omitted_fields;
    extern const SettingsNonZeroUInt64 max_insert_block_size;
    extern const SettingsAggregateFunctionInputFormat aggregate_function_input_format;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_USAGE_OF_INPUT;
    extern const int UNKNOWN_TYPE_OF_QUERY;
}

InputFormatPtr getInputFormatFromASTInsertQuery(
    const ASTPtr & ast,
    bool with_buffers,
    const Block & header,
    ContextPtr context,
    const ASTPtr & input_function)
{
    /// get ast query
    const auto * ast_insert_query = ast->as<ASTInsertQuery>();

    if (!ast_insert_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query requires data to insert, but it is not INSERT query");

    if (ast_insert_query->infile && context->getApplicationType() == Context::ApplicationType::SERVER)
        throw Exception(ErrorCodes::UNKNOWN_TYPE_OF_QUERY, "Query has infile and was send directly to server");

    if (ast_insert_query->format.empty())
    {
        if (input_function)
            throw Exception(ErrorCodes::INVALID_USAGE_OF_INPUT, "FORMAT must be specified for function input()");
        throw Exception(ErrorCodes::LOGICAL_ERROR, "INSERT query requires format to be set");
    }

    std::unique_ptr<ReadBuffer> input_buffer = with_buffers
        ? getReadBufferFromASTInsertQuery(ast)
        : std::make_unique<EmptyReadBuffer>();

    /// Create a source from input buffer using format from query
    auto format = context->getInputFormat(ast_insert_query->format, *input_buffer, header, context->getSettingsRef()[Setting::max_insert_block_size]);
    format->addBuffer(std::move(input_buffer));
    return format;
}

Pipe getSourceFromInputFormat(
    const ASTPtr & ast,
    InputFormatPtr format,
    ContextPtr context,
    const ASTPtr & input_function)
{
    Pipe pipe(format);

    const auto * ast_insert_query = ast->as<ASTInsertQuery>();
    if (context->getSettingsRef()[Setting::input_format_defaults_for_omitted_fields] && ast_insert_query->table_id && !input_function)
    {
        StoragePtr storage = DatabaseCatalog::instance().getTable(ast_insert_query->table_id, context);
        auto metadata_snapshot = storage->getInMemoryMetadataPtr();
        const auto & columns = metadata_snapshot->getColumns();
        if (columns.hasDefaults())
        {
            pipe.addSimpleTransform([&](const SharedHeader & cur_header)
            {
                return std::make_shared<AddingDefaultsTransform>(cur_header, columns, *format, context);
            });
        }
    }

    return pipe;
}

Pipe getSourceFromASTInsertQuery(
    const ASTPtr & ast,
    bool with_buffers,
    const Block & header,
    ContextPtr context,
    const ASTPtr & input_function)
{
    auto format = getInputFormatFromASTInsertQuery(ast, with_buffers, header, context, input_function);
    return getSourceFromInputFormat(ast, std::move(format), std::move(context), input_function);
}

std::unique_ptr<ReadBuffer> getReadBufferFromASTInsertQuery(const ASTPtr & ast)
{
    const auto * insert_query = ast->as<ASTInsertQuery>();
    if (!insert_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query requires data to insert, but it is not INSERT query");

    if (insert_query->infile)
    {
        /// Data can be from infile
        const auto & in_file_node = insert_query->infile->as<ASTLiteral &>();
        const auto in_file = in_file_node.value.safeGet<std::string>();

        /// It can be compressed and compression method maybe specified in query
        std::string compression_method;
        if (insert_query->compression)
        {
            const auto & compression_method_node = insert_query->compression->as<ASTLiteral &>();
            compression_method = compression_method_node.value.safeGet<std::string>();
        }

        /// Otherwise, it will be detected from file name automatically (by chooseCompressionMethod)
        /// Buffer for reading from file is created and wrapped with appropriate compression method
        return wrapReadBufferWithCompressionMethod(std::make_unique<ReadBufferFromFile>(in_file), chooseCompressionMethod(in_file, compression_method));
    }

    std::vector<ReadBufferUniquePtr> buffers;
    if (insert_query->data)
    {
        /// Data could be in parsed (ast_insert_query.data) and in not parsed yet (input_buffer_tail_part) part of query.
        auto ast_buffer = std::make_unique<ReadBufferFromMemory>(
            insert_query->data, insert_query->end - insert_query->data);

        buffers.emplace_back(std::move(ast_buffer));
    }

    /// tail does not possess the input buffer
    if (insert_query->tail)
    {
        buffers.emplace_back(wrapReadBufferPointer(insert_query->tail));
        insert_query->tail.reset();
    }

    chassert(!buffers.empty());
    return std::make_unique<ConcatReadBuffer>(std::move(buffers));
}

std::pair<Block, bool> transformAggregateColumnsToValues(ContextPtr context, const Block & header)
{
    auto format = context->getSettingsRef()[Setting::aggregate_function_input_format];
    if (format == FormatSettings::AggregateFunctionInputFormat::State)
        return {header, false};

    Block new_header;
    bool changed = false;
    // TODO subcolumns
    for (const auto & col : header)
    {
        if (const auto * func = dynamic_cast<const DataTypeAggregateFunction*>(col.type.get()))
        {
            changed = true;
            const auto & args = func->getFunction()->getArgumentTypes();
            auto value_type = args.size() > 1 ? std::make_shared<DataTypeTuple>(args) : args.at(0);
            if (format == FormatSettings::AggregateFunctionInputFormat::Value)
                new_header.insert(ColumnWithTypeAndName(value_type, col.name));
            else
                new_header.insert(ColumnWithTypeAndName(std::make_shared<const DataTypeArray>(value_type), col.name));
        }
        else
            new_header.insert(col);
    }

    return {new_header, changed};
}

ProcessorPtr createAggregationFromValuesProcessor(ContextPtr context, const SharedHeader & format_header, const Block & header)
{
    chassert(format_header->columns() == header.columns());
    chassert(context->getSettingsRef()[Setting::aggregate_function_input_format] != FormatSettings::AggregateFunctionInputFormat::State);
    bool is_array = context->getSettingsRef()[Setting::aggregate_function_input_format] == FormatSettings::AggregateFunctionInputFormat::Array;

    ActionsDAG dag(format_header->getNamesAndTypesList());
    for (size_t pos = 0; pos < header.columns(); ++pos)
    {
        const auto & agg_column = header.getByPosition(pos);
        const auto & value_column = format_header->getByPosition(pos);
        const auto * func = dynamic_cast<const DataTypeAggregateFunction*>(agg_column.type.get());
        if (func && agg_column.type != value_column.type)
        {
            auto init_aggr = FunctionFactory::instance().get("initializeAggregation", context);

            //calculateConstantActionNodeName
            auto agg_func_name = std::format("{}State{}", func->getFunctionName(), is_array ? "Array" : "");
            ColumnWithTypeAndName agg_func_name_column;
            agg_func_name_column.name = calculateConstantActionNodeName(agg_func_name);
            agg_func_name_column.column = DataTypeString().createColumnConst(0, agg_func_name);
            agg_func_name_column.type = std::make_shared<DataTypeString>();

            ActionsDAG::NodeRawConstPtrs args = {&dag.addColumn(std::move(agg_func_name_column))};
            auto args_size = func->getArgumentsDataTypes().size();
            args.reserve(1 + args_size);
            if (args_size == 1)
                args.push_back(dag.getOutputs()[pos]);
            else
            {
                auto args_tuple = dag.getOutputs()[pos];
                auto tuple_element_func = FunctionFactory::instance().get("tupleElement", context);
                for (size_t arg_num = 0; arg_num < args_size; ++arg_num)
                {
                    ColumnWithTypeAndName tuple_index_column;
                    tuple_index_column.name = calculateConstantActionNodeName(arg_num);
                    tuple_index_column.column = DataTypeUInt64().createColumnConst(0, arg_num + 1);
                    tuple_index_column.type = std::make_shared<DataTypeUInt64>();

                    args.push_back(&dag.addFunction(tuple_element_func, {args_tuple, &dag.addColumn(std::move(tuple_index_column))}, {}));
                }
            }

            dag.getOutputs()[pos] = &dag.addFunction(init_aggr, std::move(args), agg_column.name);
        }
    }

   return std::make_shared<ExpressionTransform>(format_header, std::make_shared<ExpressionActions>(std::move(dag)));
}


}
