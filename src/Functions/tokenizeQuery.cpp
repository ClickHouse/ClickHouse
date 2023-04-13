#include <Columns/ColumnArray.h>

#include <DataTypes/DataTypeArray.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Parsers/queryNormalization.h>
#include <base/find_symbols.h>
#include <Common/StringUtils/StringUtils.h>

#include <functional>
#include <iostream>
#include <string_view>
#include <boost/program_options.hpp>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/obfuscateQueries.h>
#include <Parsers/parseQuery.h>
#include <Common/ErrorCodes.h>
#include <Common/TerminalSize.h>

#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/StorageFactory.h>
#include <Storages/registerStorages.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerFormats.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
class TokenizeQueryImpl : public IFunction
{

public:
    static constexpr auto name = "tokenizeQuery";

    String getName() const override { return name; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<TokenizeQueryImpl>(); }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires one argument: query to be tokenized", getName());

        if (!isStringOrFixedString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must have string type", getName());

        std::vector<DataTypePtr> tupleTypes(3);
        tupleTypes = {std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt32>(), std::make_shared<DataTypeUInt32>()};
        return std::make_shared<DataTypeTuple>(tupleTypes);
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override { return false; }

    using column_type = ColumnArray;
    using data_type = DataTypeArray;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        auto query = getStringColumnValue(arguments[0].column);
        Lexer lexer(query.data(), query.data() + query.size());
        Token token = lexer.nextToken();
        size_t iter = 0;

        auto col_0 = ColumnString::create();
        auto col_1 = ColumnUInt32::create();
        auto col_2 = ColumnUInt32::create();

        ColumnString::Chars & data = col_0->getChars();
        ColumnString::Offsets & offsets = col_0->getOffsets();
        typename ColumnUInt32::Container & vec_res_1 = col_1->getData();
        typename ColumnUInt32::Container & vec_res_2 = col_2->getData();
        data.reserve(query.size());
        for (size_t i = 0; i < query.size(); ++i)
        {
            data[i] = query[i];
        }
        // memcpySmallAllowReadWriteOverflow15(data.data(), query.c_str(), query.size());
        size_t last_offset = 0;
        while (!token.isEnd()) 
        {
            offsets.resize(iter + 1);
            if (token.size() != 0)
            {
            offsets[iter] = last_offset + token.size() + 1;
            last_offset = offsets[iter];
            } else 
            {
                offsets[iter] = last_offset;
            }
            vec_res_1.resize(iter + 1);
            vec_res_1[iter] = static_cast<uint32_t>(lexer.getPos() - lexer.getBegin() - token.size()) + 1;

            vec_res_2.resize(iter + 1);
            vec_res_2[iter] = static_cast<uint32_t>(lexer.getEnd() - lexer.getPos());

            token = lexer.nextToken();
            ++iter;
        } 
        Columns tuple_columns(3);
        tuple_columns[0] = std::move(col_0);
        tuple_columns[1] = std::move(col_1);
        tuple_columns[2] = std::move(col_2);
        auto tuple =  ColumnTuple::create(std::move(tuple_columns));

        auto offsets_arr = ColumnArray::ColumnOffsets::create(offsets.size());
        auto & arr_out_offsets = offsets_arr->getData();
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            arr_out_offsets[i] = i + 1;
        }
        return tuple;
    }
private:
    String getStringColumnValue(const ColumnPtr & column) const
    { 
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            const ColumnString::Chars & data = *(&col->getChars());
            const ColumnString::Offsets & offsets = *(&col->getOffsets());
            return reinterpret_cast<const char *>(data.data() + offsets[- 1]);
        }
        if (const ColumnFixedString * fixed_col = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            const ColumnString::Chars & data = fixed_col->getChars();
            return reinterpret_cast<const char *>(data.data());
        }
        if (const ColumnConst * const_col = checkAndGetColumnConstStringOrFixedString(column.get()))
        {
            return const_col->getValue<String>();
        }
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
            column->getName(), getName()); 
    } 
};
}

REGISTER_FUNCTION(TokenizeQuery)
{
    factory.registerFunction<TokenizeQueryImpl>({}, FunctionFactory::CaseInsensitive);
}

}


