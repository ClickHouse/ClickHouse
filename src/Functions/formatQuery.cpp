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
    extern const int INVALID_FORMAT_INSERT_QUERY_WITH_DATA;
}

namespace
{

template <bool oneline>
struct Format
{
    static constexpr auto name = oneline ? "formatQueryOneLine" : "formatQuery";
    static constexpr size_t max_query_size = 32768;
    static constexpr size_t max_parser_depth = 2048;
    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        std::string_view query(reinterpret_cast<const char *>(data.data() + offsets[- 1]), offsets[0] - offsets[- 1] - 1);
        const char * pos = query.data();
        const char * end = pos + query.size();

        WriteBufferFromOwnString buffer;
        ParserQuery parser(end, false);
        ASTPtr res = parseQueryAndMovePosition(
            parser, pos, end, "query", false, max_query_size, max_parser_depth);
        /// For insert query with data(INSERT INTO ... VALUES ...), will lead to format fail,
        /// should throw exception early and make exception message more readable.
        if (const auto * insert_query = res->as<ASTInsertQuery>(); insert_query && insert_query->data)
        {
            throw Exception(DB::ErrorCodes::INVALID_FORMAT_INSERT_QUERY_WITH_DATA,
                "Can't format ASTInsertQuery with data, since data will be lost");
        }
        
        formatAST(*res, buffer, false, oneline);
        buffer.next();

        std::string result = buffer.str();
        // Formated queries are usually don't take more than x3 characters.
        res_data.reserve(result.size() * 3);
        memcpySmallAllowReadWriteOverflow15(res_data.data(), result.c_str(), result.size());
        res_offsets.resize(1);
        res_offsets[0] = result.size() + 1;
    }
    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot apply function formatQuery to fixed string.");
    }
};
}

REGISTER_FUNCTION(FormatQuery)
{
    factory.registerFunction<FunctionStringToString<Format<true>, Format<true>>>();
    factory.registerFunction<FunctionStringToString<Format<false>, Format<false>>>();
}

}
