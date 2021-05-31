#include <iostream>
#include <fstream>
#include <string>
#include <cstdio>
#include <time.h>
#include <filesystem>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    /// How to test:
    /// build ClickHouse with YAML_fuzzer.cpp
    /// ./YAML_fuzzer YAML_CORPUS
    /// where YAML_CORPUS is a directory with different YAML configs for libfuzzer
    char buf[L_tmpnam];
    char* file_name = std::tmpnam(buf);
    if (file_name == nullptr)
    {
        std::cerr << "Cannot create temp file!\n";
        return 1;
    }
    std::string cur_file(file_name);

    std::string input = std::string(reinterpret_cast<const char*>(data), size);
    DB::YAMLParser parser;

    {
        std::ofstream temp_file(cur_file);
        temp_file << input;
    }

    try
    {
        DB::YAMLParser::parse(cur_file);
    }
    catch (...)
    {
        std::cerr << "YAML_fuzzer failed: " << getCurrentExceptionMessage() << std::endl;
        return 1;
    }
    return 0;
}

