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
}

namespace
{

struct Obfuscate
{
    static constexpr auto name = "obfuscateQuery";
    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        WordMap obfuscated_words_map;
        WordSet used_nouns;
        SipHash hash_func;

        // if (options.count("seed"))
        // {
        //     std::string seed;
        //     hash_func.update(options["seed"].as<std::string>());
        // }
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

        size_t size = offsets.size();
        res_offsets.resize(size + 1);
            
        WriteBufferFromOwnString buffer;
        std::string_view query(reinterpret_cast<const char *>(data.data() + offsets[- 1]), offsets[0] - offsets[- 1] - 1);
        obfuscateQueries(query, buffer, obfuscated_words_map, used_nouns, hash_func, is_known_identifier);

        std::string res = buffer.str();
        // Obfuscated queries are usually don't take more than x2 characters.
        res_data.reserve(res.size());
        for (size_t i = 0; i < res.size(); ++i) {
            res_data[i] = res[i];
        }
        res_offsets[-1] = 0;
        res_offsets[0] = res.size() + 1;
        // for (size_t i = 0; i < size; ++i) {
        //     res_offsets[i] = offsets[i];
        // }
        // res_offsets[0] = res_data.size();

        // for (size_t j = 0; j < res.size(); ++j) {
        //     res_data[j] = res[j];
        // }
        // if (res_data[0]) {
        //     throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{}, {}, {}, {}, {}::: {}, {}, {}, {}.", res, res_offsets.size(), res_offsets[-1], res_offsets[0], res_offsets[1], offsets.size(), offsets[-1], offsets[0], offsets[1]);
        // }
        // for (size_t i = 0; i < size; ++i)
        // {
        //     WriteBufferFromOwnString buffer;
        //     std::string_view query(reinterpret_cast<const char *>(data.data() + offsets[i - 1]), offsets[i] - offsets[i - 1] - 1);
        //     obfuscateQueries(query, buffer, obfuscated_words_map, used_nouns, hash_func, is_known_identifier);

        //     std::string res = buffer.str();
        //     for (size_t j = 0; j < res.size(); ++j) {
        //         res_data[j + offsets[i - 1]] = res[j];
        //     }
        //     res_offsets[i] = res_data.size();
        // }
        
    }
    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot apply function normalizeUTF8 to fixed string.");
    }
};

}

REGISTER_FUNCTION(ObfuscateQuery)
{
    factory.registerFunction<FunctionStringToString<Obfuscate, Obfuscate>>();
}

}

            // auto col_to = ColumnString::create();
            // ColumnString::Chars & data_to = col_to->getChars();
            // ColumnString::Offsets & offsets_to = col_to->getOffsets();
