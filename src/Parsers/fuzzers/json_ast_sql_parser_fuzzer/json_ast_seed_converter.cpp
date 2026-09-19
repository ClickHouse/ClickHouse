/// Helper for `json_ast_sql_parser_fuzzer`: converts between the JSON AST text produced by
/// `parseQueryToJSON` (or `EXPLAIN AST`-style tooling) and the binary protobuf corpus format.
///
///     json_ast_seed_converter to-proto <in.json|-> <out.bin>
///     json_ast_seed_converter to-json <in.bin|->
///
/// `to-json` renders a corpus or crash input as the JSON document the fuzzer would feed to
/// `IAST::createFromJSON`; it can then be passed to `formatQueryFromJSON` or executed with the
/// `clickhouse_json` dialect.

#include <Common/Exception.h>

#include <Parsers/fuzzers/json_ast_sql_parser_fuzzer/JSONASTProtoConverter.h>
#include <json_ast.pb.h>

#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

namespace
{

std::string readAll(const std::string & path)
{
    if (path == "-")
        return std::string(std::istreambuf_iterator<char>(std::cin), std::istreambuf_iterator<char>());
    std::ifstream in(path, std::ios::binary);
    if (!in)
        throw std::runtime_error("Cannot open " + path);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

int usage()
{
    std::cerr << "Usage:\n"
        "  json_ast_seed_converter to-proto <in.json|-> <out.bin>\n"
        "  json_ast_seed_converter to-json <in.bin|->\n";
    return 2;
}

}

int main(int argc, char ** argv)
try
{
    if (argc < 3)
        return usage();

    std::string mode = argv[1];
    if (mode == "to-proto")
    {
        if (argc != 4)
            return usage();
        json_ast_fuzzer::Node node;
        DB::JSONASTFuzzer::jsonToProto(readAll(argv[2]), node);
        std::ofstream out(argv[3], std::ios::binary | std::ios::trunc);
        if (!out || !node.SerializeToOstream(&out))
            throw std::runtime_error(std::string("Cannot write ") + argv[3]);
        return 0;
    }
    if (mode == "to-json")
    {
        if (argc != 3)
            return usage();
        json_ast_fuzzer::Node node;
        std::string data = readAll(argv[2]);
        if (!node.ParseFromString(data))
            throw std::runtime_error("The input is not a valid binary json_ast_fuzzer.Node message");
        std::string json;
        DB::JSONASTFuzzer::ProtoToJSONLimits limits;
        limits.max_depth = 1 << 16;
        limits.max_output_bytes = 1 << 30;
        if (!DB::JSONASTFuzzer::protoToJSON(node, limits, json))
            throw std::runtime_error("The message exceeds the rendering limits");
        std::cout << json << '\n';
        return 0;
    }
    return usage();
}
catch (const DB::Exception & e)
{
    std::cerr << e.displayText() << '\n';
    return 1;
}
catch (const std::exception & e)
{
    std::cerr << e.what() << '\n';
    return 1;
}
