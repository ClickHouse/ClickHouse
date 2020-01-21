#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>

#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromHTTP.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatFactory.h>
#include "registerFunctions.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_CAST;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int NETWORK_ERROR;
}

/**
 * This function provides possibility to map rows via external HTTP-call
 * httpFunc('http://localhost/address', 'RowBinary', 'JSON', 'Array(Nullable(UInt32)', arg1, arg2, arg3)
 * arguments:
 * address of endpoint
 * input format
 * output format
 * return value format
 * variadic list of arguments
 */
class FunctionHttpFunc : public IFunction
{
public:
    static constexpr auto name = "httpFunc";

    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionHttpFunc>(context); }

    explicit FunctionHttpFunc(const Context & context_) : context(context_) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 4; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const final { return {0, 1, 2, 3}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        // need at least one variadic argument
        if (arguments.size() < 5)
            throw Exception(
                "Function " + getName() + " need at least 5 arguments, " + toString(arguments.size()) + " given",
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);

        for (size_t i = 0; i <= 3; i++)
        {
            if (!isString(arguments[i].type))
                throw Exception(
                    "Illegal type " + arguments[i].type->getName() + " of " + toString(i + 1) + " argument of function " + getName()
                        + ", expected a string.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        // infer return type
        const ColumnConst * return_type_col = checkAndGetColumnConstStringOrFixedString(arguments[3].column.get());
        if (!return_type_col)
            throw Exception("Fourth argument of function " + getName() + " must be a constant string", ErrorCodes::ILLEGAL_COLUMN);

        return getReturnTypeFromArgument(return_type_col);
    }

    DataTypePtr getReturnTypeFromArgument(const ColumnConst * column) const
    {
        auto return_type_name = column->getValue<String>();
        auto structure = "column_name " + return_type_name;
        ColumnsDescription parsed_columns = parseColumnsListFromString(structure, context);
        if (!parsed_columns.has("column_name"))
            throw Exception(
                "Failed to parse return value of function " + getName() + " from '" + return_type_name + "'",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return parsed_columns.get("column_name").type;
    }

    String getFormatFromArgument(const IColumn * column, const String & format_direction) const
    {
        const auto const_column = checkAndGetColumnConstStringOrFixedString(column);
        if (!const_column)
            throw Exception(
                "Failed to get " + format_direction + " format for function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return const_column->getValue<String>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isVariadic() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        // resulting column
        const ColumnConst * result_type_column = checkAndGetColumnConstStringOrFixedString(block.getByPosition(arguments[3]).column.get());
        if (!result_type_column)
            throw Exception("Bad fourth argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypePtr & return_type = getReturnTypeFromArgument(result_type_column);

        // no need for communication on empty block
        if (input_rows_count == 0)
        {
            block.getByPosition(result).column = return_type->createColumn();
            return;
        }

        // endpoint address
        const auto url_column = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get());
        if (!url_column)
            throw Exception("Bad first argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        Poco::URI uri(url_column->getValue<String>());

        // RPC communication format
        const String rpc_input_format = getFormatFromArgument(block.getByPosition(arguments[1]).column.get(), "input");
        const String rps_output_format = getFormatFromArgument(block.getByPosition(arguments[2]).column.get(), "output");

        // sample block for results of invocation
        // @todo for JSON formats, we need to return pre-defined name of the field here. need to select appropriate
        Block result_block;
        result_block.insert(ColumnWithTypeAndName(return_type, "PICK_UP_SUITABLE_NAME_HERE"));

        // preparing request
        Block argument_block;
        for (size_t arg = 4; arg < arguments.size(); arg++)
        {
            argument_block.insert(block.getByPosition(arguments[arg]));
        }

        ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [&](std::ostream & ostr)
        {
            WriteBufferFromOStream out_buffer(ostr);
            auto output = context.getOutputFormat(rpc_input_format, out_buffer, argument_block.cloneEmpty());
            output->writePrefix();
            output->write(argument_block);
            output->writeSuffix();
            output->flush();
        };

        std::unique_ptr<ReadWriteBufferFromHTTP> in_ptr;

        try
        {
            in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(
                uri,
                Poco::Net::HTTPRequest::HTTP_POST,
                out_stream_callback,
                ConnectionTimeouts::getHTTPTimeouts(context),
                0,
                Poco::Net::HTTPBasicCredentials{},
                DBMS_DEFAULT_BUFFER_SIZE,
                ReadWriteBufferFromHTTP::HTTPHeaderEntries{});
        }
        catch (std::exception & e)
        {
            throw Exception(
                "Error communicating " + uri.toString() + " in function " + getName() + ": " + e.what(), ErrorCodes::NETWORK_ERROR);
        }

        auto input_stream
            = context.getInputFormat(rps_output_format, *in_ptr, result_block.cloneEmpty(), context.getSettingsRef().max_block_size);
        auto block_input_stream = OwningBlockInputStream<ReadWriteBufferFromHTTP>(input_stream, std::move(in_ptr));
        block_input_stream.readPrefix();

        MutableColumnPtr column_mixed = return_type->createColumn();

        while (const auto answer_block = block_input_stream.read())
        {
            const ColumnWithTypeAndName & answer_column = answer_block.getByPosition(0);
            size_t block_rows = answer_block.rows();
            column_mixed->insertRangeFrom(*answer_column.column, 0, block_rows);
        }
        block_input_stream.readSuffix();

        if (column_mixed->size() != input_rows_count)
            throw Exception(
                "Incorrect number of rows returned from " + uri.toString() + " in function " + getName() + ": "
                    + toString(column_mixed->size()) + " != " + toString(input_rows_count),
                ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        block.getByPosition(result).column = std::move(column_mixed);
    }

private:
    const Context & context;
};

void registerFunctionHttpFunc(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHttpFunc>();
}

}
