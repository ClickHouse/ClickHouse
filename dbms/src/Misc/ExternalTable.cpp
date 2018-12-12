#include "ExternalTable.h"

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <IO/copyData.h>
//#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadBufferFromFile.h>
//#include <IO/LimitReadBuffer.h>
//#include <Storages/StorageMemory.h>
//#include <Poco/Net/MessageHeader.h>



namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ExternalTableData BaseExternalTable::getData(const Context & context)
{
    initReadBuffer();
    initSampleBlock();
    auto input = context.getInputFormat(format, *read_buffer, sample_block, DEFAULT_BLOCK_SIZE);
    return std::make_pair(std::make_shared<AsynchronousBlockInputStream>(input), name);
}

void BaseExternalTable::clean()
{
    name = "";
    file = "";
    format = "";
    structure.clear();
    sample_block = Block();
    read_buffer.reset();
}

/// Function for debugging information output
void BaseExternalTable::write()
{
    std::cerr << "file " << file << std::endl;
    std::cerr << "name " << name << std::endl;
    std::cerr << "format " << format << std::endl;
    std::cerr << "structure: \n";
    for (size_t i = 0; i < structure.size(); ++i)
        std::cerr << "\t" << structure[i].first << " " << structure[i].second << std::endl;
}

std::vector<std::string> BaseExternalTable::split(const std::string & s, const std::string & d)
{
    std::vector<std::string> res;
    boost::split(res, s, boost::algorithm::is_any_of(d), boost::algorithm::token_compress_on);
    return res;
}

void BaseExternalTable::parseStructureFromStructureField(const std::string & argument)
{
    std::vector<std::string> vals = split(argument, " ,");

    if (vals.size() & 1)
        throw Exception("Odd number of attributes in section structure", ErrorCodes::BAD_ARGUMENTS);

    for (size_t i = 0; i < vals.size(); i += 2)
        structure.emplace_back(vals[i], vals[i + 1]);
}

void BaseExternalTable::parseStructureFromTypesField(const std::string & argument)
{
    std::vector<std::string> vals = split(argument, " ,");

    for (size_t i = 0; i < vals.size(); ++i)
        structure.emplace_back("_" + toString(i + 1), vals[i]);
}

void BaseExternalTable::initSampleBlock()
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    for (size_t i = 0; i < structure.size(); ++i)
    {
        ColumnWithTypeAndName column;
        column.name = structure[i].first;
        column.type = data_type_factory.get(structure[i].second);
        column.column = column.type->createColumn();
        sample_block.insert(std::move(column));
    }
}


void ExternalTable::initReadBuffer()
{
    if (file == "-")
        read_buffer = std::make_unique<ReadBufferFromFileDescriptor>(STDIN_FILENO);
    else
        read_buffer = std::make_unique<ReadBufferFromFile>(file);
}

ExternalTable::ExternalTable(const boost::program_options::variables_map & external_options)
{
    if (external_options.count("file"))
        file = external_options["file"].as<std::string>();
    else
        throw Exception("--file field have not been provided for external table", ErrorCodes::BAD_ARGUMENTS);

    if (external_options.count("name"))
        name = external_options["name"].as<std::string>();
    else
        throw Exception("--name field have not been provided for external table", ErrorCodes::BAD_ARGUMENTS);

    if (external_options.count("format"))
        format = external_options["format"].as<std::string>();
    else
        throw Exception("--format field have not been provided for external table", ErrorCodes::BAD_ARGUMENTS);

    if (external_options.count("structure"))
        parseStructureFromStructureField(external_options["structure"].as<std::string>());
    else if (external_options.count("types"))
        parseStructureFromTypesField(external_options["types"].as<std::string>());
    else
        throw Exception("Neither --structure nor --types have not been provided for external table", ErrorCodes::BAD_ARGUMENTS);
}

}
