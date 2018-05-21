#pragma once

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <IO/copyData.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadBufferFromFile.h>
#include <Storages/StorageMemory.h>
#include <Client/Connection.h>
#include <Poco/Net/HTMLForm.h>
#include <Poco/Net/PartHandler.h>
#include <Poco/Net/MessageHeader.h>
#include <Common/HTMLForm.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


/// The base class containing the basic information about external table and
/// basic functions for extracting this information from text fields.
class BaseExternalTable
{
public:
    std::string file;       /// File with data or '-' if stdin
    std::string name;       /// The name of the table
    std::string format;     /// Name of the data storage format

    /// Description of the table structure: (column name, data type name)
    std::vector<std::pair<std::string, std::string>> structure;

    std::unique_ptr<ReadBuffer> read_buffer;
    Block sample_block;

    virtual ~BaseExternalTable() {};

    /// Initialize read_buffer, depending on the data source. By default, does nothing.
    virtual void initReadBuffer() {};

    /// Get the table data - a pair (a thread with the contents of the table, the name of the table)
    ExternalTableData getData(const Context & context)
    {
        initReadBuffer();
        initSampleBlock();
        ExternalTableData res = std::make_pair(std::make_shared<AsynchronousBlockInputStream>(context.getInputFormat(
            format, *read_buffer, sample_block, DEFAULT_BLOCK_SIZE)), name);
        return res;
    }

protected:
    /// Clear all accumulated information
    void clean()
    {
        name = "";
        file = "";
        format = "";
        structure.clear();
        sample_block = Block();
        read_buffer.reset();
    }

    /// Function for debugging information output
    void write()
    {
        std::cerr << "file " << file << std::endl;
        std::cerr << "name " << name << std::endl;
        std::cerr << "format " << format << std::endl;
        std::cerr << "structure: \n";
        for (size_t i = 0; i < structure.size(); ++i)
            std::cerr << "\t" << structure[i].first << " " << structure[i].second << std::endl;
    }

    static std::vector<std::string> split(const std::string & s, const std::string & d)
    {
        std::vector<std::string> res;
        boost::split(res, s, boost::algorithm::is_any_of(d), boost::algorithm::token_compress_on);
        return res;
    }

    /// Construct the `structure` vector from the text field `structure`
    virtual void parseStructureFromStructureField(const std::string & argument)
    {
        std::vector<std::string> vals = split(argument, " ,");

        if (vals.size() & 1)
            throw Exception("Odd number of attributes in section structure", ErrorCodes::BAD_ARGUMENTS);

        for (size_t i = 0; i < vals.size(); i += 2)
            structure.emplace_back(vals[i], vals[i + 1]);
    }

    /// Construct the `structure` vector from the text field `types`
    virtual void parseStructureFromTypesField(const std::string & argument)
    {
        std::vector<std::string> vals = split(argument, " ,");

        for (size_t i = 0; i < vals.size(); ++i)
            structure.emplace_back("_" + toString(i + 1), vals[i]);
    }

private:
    /// Initialize sample_block according to the structure of the table stored in the `structure`
    void initSampleBlock()
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
};


/// Parsing of external table used in the tcp client.
class ExternalTable : public BaseExternalTable
{
public:
    void initReadBuffer() override
    {
        if (file == "-")
            read_buffer = std::make_unique<ReadBufferFromFileDescriptor>(STDIN_FILENO);
        else
            read_buffer = std::make_unique<ReadBufferFromFile>(file);
    }

    /// Extract parameters from variables_map, which is built on the client command line
    ExternalTable(const boost::program_options::variables_map & external_options)
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
};

/// Parsing of external table used when sending tables via http
/// The `handlePart` function will be called for each table passed,
/// so it's also necessary to call `clean` at the end of the `handlePart`.
class ExternalTablesHandler : public Poco::Net::PartHandler, BaseExternalTable
{
public:
    std::vector<std::string> names;

    ExternalTablesHandler(Context & context_, Poco::Net::NameValueCollection params_) : context(context_), params(params_) { }

    void handlePart(const Poco::Net::MessageHeader & header, std::istream & stream)
    {
        /// The buffer is initialized here, not in the virtual function initReadBuffer
        read_buffer = std::make_unique<ReadBufferFromIStream>(stream);

        /// Retrieve a collection of parameters from MessageHeader
        Poco::Net::NameValueCollection content;
        std::string label;
        Poco::Net::MessageHeader::splitParameters(header.get("Content-Disposition"), label, content);

        /// Get parameters
        name = content.get("name", "_data");
        format = params.get(name + "_format", "TabSeparated");

        if (params.has(name + "_structure"))
            parseStructureFromStructureField(params.get(name + "_structure"));
        else if (params.has(name + "_types"))
            parseStructureFromTypesField(params.get(name + "_types"));
        else
            throw Exception("Neither structure nor types have not been provided for external table " + name + ". Use fields " + name + "_structure or " + name + "_types to do so.", ErrorCodes::BAD_ARGUMENTS);

        ExternalTableData data = getData(context);

        /// Create table
        NamesAndTypesList columns = sample_block.getNamesAndTypesList();
        StoragePtr storage = StorageMemory::create(data.second, ColumnsDescription{columns});
        storage->startup();
        context.addExternalTable(data.second, storage);
        BlockOutputStreamPtr output = storage->write(ASTPtr(), context.getSettingsRef());

        /// Write data
        data.first->readPrefix();
        output->writePrefix();
        while(Block block = data.first->read())
            output->write(block);
        data.first->readSuffix();
        output->writeSuffix();

        names.push_back(name);
        /// We are ready to receive the next file, for this we clear all the information received
        clean();
    }

private:
    Context & context;
    Poco::Net::NameValueCollection params;
};


}
