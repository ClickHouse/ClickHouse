#include <boost/program_options.hpp>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/IStorage.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/LimitReadBuffer.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Core/ExternalTable.h>
#include <Poco/Net/MessageHeader.h>
#include <base/scope_guard.h>


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
    auto input = context->getInputFormat(format, *read_buffer, sample_block, context->getSettingsRef().get("max_block_size").get<UInt64>());

    auto data = std::make_unique<ExternalTableData>();
    data->pipe = std::make_unique<QueryPipelineBuilder>();
    data->pipe->init(Pipe(std::move(input)));
    data->table_name = name;

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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Odd number of attributes in section structure: {}", vals.size());

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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "--file field have not been provided for external table");

    if (external_options.count("name"))
        name = external_options["name"].as<std::string>();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "--name field have not been provided for external table");

    if (external_options.count("format"))
        format = external_options["format"].as<std::string>();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "--format field have not been provided for external table");

    if (external_options.count("structure"))
        parseStructureFromStructureField(external_options["structure"].as<std::string>());
    else if (external_options.count("types"))
        parseStructureFromTypesField(external_options["types"].as<std::string>());
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Neither --structure nor --types have not been provided for external table");
}


void ExternalTablesHandler::handlePart(const Poco::Net::MessageHeader & header, ReadBuffer & stream)
{
    /// After finishing this function we will be ready to receive the next file, for this we clear all the information received.
    /// We should use SCOPE_EXIT because read_buffer should be reset correctly if there will be an exception.
    SCOPE_EXIT(clear());

    const Settings & settings = getContext()->getSettingsRef();

    if (settings.http_max_multipart_form_data_size)
        read_buffer = std::make_unique<LimitReadBuffer>(
            stream, settings.http_max_multipart_form_data_size,
            /* trow_exception */ true, /* exact_limit */ std::optional<size_t>(),
            "the maximum size of multipart/form-data. "
            "This limit can be tuned by 'http_max_multipart_form_data_size' setting");
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Neither structure nor types have not been provided for external table {}. "
                        "Use fields {}_structure or {}_types to do so.", name, name, name);

    ExternalTableDataPtr data = getData(getContext());

    /// Create table
    NamesAndTypesList columns = sample_block.getNamesAndTypesList();
    auto temporary_table = TemporaryTableHolder(getContext(), ColumnsDescription{columns}, {});
    auto storage = temporary_table.getTable();
    getContext()->addExternalTable(data->table_name, std::move(temporary_table));
    auto sink = storage->write(ASTPtr(), storage->getInMemoryMetadataPtr(), getContext(), /*async_insert=*/false);

    /// Write data
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*data->pipe));
    pipeline.complete(std::move(sink));
    pipeline.setNumThreads(1);

    CompletedPipelineExecutor executor(pipeline);
    executor.execute();
}

}
