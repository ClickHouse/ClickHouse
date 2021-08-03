#include <boost/program_options.hpp>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/IStorage.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/LimitReadBuffer.h>

#include <Processors/Pipe.h>
#include <Processors/Sources/SinkToOutputStream.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Sources/SourceFromInputStream.h>

#include <Core/ExternalTable.h>
#include <Poco/Net/MessageHeader.h>
#include <Formats/FormatFactory.h>
#include <common/find_symbols.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


ExternalTableDataPtr BaseExternalTable::getData(ContextPtr context)
{
    initReadBuffer();
    initSampleBlock();
    auto input = context->getInputFormat(format, *read_buffer, sample_block, DEFAULT_BLOCK_SIZE);
    auto stream = std::make_shared<AsynchronousBlockInputStream>(input);

    auto data = std::make_unique<ExternalTableData>();
    data->table_name = name;
    data->pipe = std::make_unique<Pipe>(std::make_shared<SourceFromInputStream>(std::move(stream)));

    return data;
}

void BaseExternalTable::clear()
{
    name.clear();
    file.clear();
    format.clear();
    structure.clear();
    sample_block.clear();
    read_buffer.reset();
}

void BaseExternalTable::parseStructureFromStructureField(const std::string & argument)
{
    std::vector<std::string> vals;
    splitInto<' ', ','>(vals, argument, true);

    if (vals.size() % 2 != 0)
        throw Exception("Odd number of attributes in section structure: " + std::to_string(vals.size()), ErrorCodes::BAD_ARGUMENTS);

    for (size_t i = 0; i < vals.size(); i += 2)
        structure.emplace_back(vals[i], vals[i + 1]);
}

void BaseExternalTable::parseStructureFromTypesField(const std::string & argument)
{
    std::vector<std::string> vals;
    splitInto<' ', ','>(vals, argument, true);

    for (size_t i = 0; i < vals.size(); ++i)
        structure.emplace_back("_" + toString(i + 1), vals[i]);
}

void BaseExternalTable::initSampleBlock()
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    for (const auto & elem : structure)
    {
        ColumnWithTypeAndName column;
        column.name = elem.first;
        column.type = data_type_factory.get(elem.second);
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


void ExternalTablesHandler::handlePart(const Poco::Net::MessageHeader & header, ReadBuffer & stream)
{
    const Settings & settings = getContext()->getSettingsRef();

    if (settings.http_max_multipart_form_data_size)
        read_buffer = std::make_unique<LimitReadBuffer>(
            stream, settings.http_max_multipart_form_data_size,
            true, "the maximum size of multipart/form-data. This limit can be tuned by 'http_max_multipart_form_data_size' setting");
    else
        read_buffer = wrapReadBufferReference(stream);

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

    ExternalTableDataPtr data = getData(getContext());

    /// Create table
    NamesAndTypesList columns = sample_block.getNamesAndTypesList();
    auto temporary_table = TemporaryTableHolder(getContext(), ColumnsDescription{columns}, {});
    auto storage = temporary_table.getTable();
    getContext()->addExternalTable(data->table_name, std::move(temporary_table));
    BlockOutputStreamPtr output = storage->write(ASTPtr(), storage->getInMemoryMetadataPtr(), getContext());

    /// Write data
    data->pipe->resize(1);

    auto sink = std::make_shared<SinkToOutputStream>(std::move(output));
    connect(*data->pipe->getOutputPort(0), sink->getPort());

    auto processors = Pipe::detachProcessors(std::move(*data->pipe));
    processors.push_back(std::move(sink));

    auto executor = std::make_shared<PipelineExecutor>(processors);
    executor->execute(/*num_threads = */ 1);

    /// We are ready to receive the next file, for this we clear all the information received
    clear();
}

}
