#include <iostream>
#include <fstream>
#include <string>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    std::string input = std::string(reinterpret_cast<const char*>(data), size);
    DB::YAMLParser parser;

    std::ofstream temp_file("YAML_fuzzer_data.yaml");
    temp_file << input;
    temp_file.close();
    
    YAMLParser::parse("YAML_fuzzer_data.yaml");

    remove("YAML_fuzzer_data.yaml");

    return 0;
}

