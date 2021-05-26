#include <iostream>
#include <fstream>
#include <string>
#include <time.h>
#include <filesystem>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    /// How to test:
    /// build ClickHouse with YAML_fuzzer.cpp
    /// ./YAML_fuzzer YAML_CORPUS
    /// where YAML_CORPUS is a directory with different YAML configs for libfuzzer

    srand(time(NULL));
    std::string cur_file = std::to_string(rand());

    while (std::filesystem::exists(cur_file))
    {
        std::string cur_file = std::to_string(rand());
    }

    std::string input = std::string(reinterpret_cast<const char*>(data), size);
    DB::YAMLParser parser;

    std::ofstream temp_file(cur_file);
    temp_file << input;
    temp_file.close();

    DB::YAMLParser::parse(cur_file);

    remove(cur_file.c_str());
    return 0;
}

