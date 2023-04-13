#include <DataTypes/DataTypeString.h>
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

class ObfuscateQueryImpl : public IFunction
{
public:
    static constexpr auto name = "obfuscateQuery";

    String getName() const override { return name; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<ObfuscateQueryImpl>(); }

    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at least one argument: the size of resulting string", getName());

        if (arguments.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at most two arguments: the size of resulting string and optional disambiguation tag", getName());

        if (!isStringOrFixedString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must have string type", getName());
        
        if (arguments.size() == 2 && !isStringOrFixedString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} must have string type", getName());
        
        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        WordMap obfuscated_words_map;
        WordSet used_nouns;
        SipHash hash_func;

        const ColumnPtr & query_column = arguments[0].column;
        String query = getStringColumnValue(query_column);
        if (arguments.size() == 2) {
            hash_func.update(getStringColumnValue(arguments[1].column));
        }

        std::unordered_set<std::string> additional_names;

        auto all_known_storage_names = StorageFactory::instance().getAllRegisteredNames();
        auto all_known_data_type_names = DataTypeFactory::instance().getAllRegisteredNames();

        additional_names.insert(all_known_storage_names.begin(), all_known_storage_names.end());
        additional_names.insert(all_known_data_type_names.begin(), all_known_data_type_names.end());

        KnownIdentifierFunc is_known_identifier = [&](std::string_view obj_name)
        {
            std::string what(obj_name);

            return FunctionFactory::instance().has(what)
                || AggregateFunctionFactory::instance().isAggregateFunctionName(what)
                || TableFunctionFactory::instance().isTableFunctionName(what)
                || FormatFactory::instance().isOutputFormat(what)
                || FormatFactory::instance().isInputFormat(what)
                || additional_names.contains(what);
        };

        auto res_col = ColumnString::create();
        ColumnString::Chars & res_data = res_col->getChars();
        ColumnString::Offsets & res_offsets = res_col->getOffsets();

        WriteBufferFromOwnString buffer;
        res_offsets.resize(1);
        obfuscateQueries(query, buffer, obfuscated_words_map, used_nouns, hash_func, is_known_identifier);

        std::string res = buffer.str();
        // Obfuscated queries are usually don't take more than x2 characters.
        res_data.reserve(res.size() * 2);

        memcpySmallAllowReadWriteOverflow15(res_data.data(), res.c_str(), res.size());
        res_offsets[0] = res.size() + 1;
        return res_col;
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

REGISTER_FUNCTION(ObfuscateQuery)
{
    factory.registerFunction<ObfuscateQueryImpl>({}, FunctionFactory::CaseInsensitive);
}

}
