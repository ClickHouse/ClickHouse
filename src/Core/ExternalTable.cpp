#include <Columns/ColumnTuple.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeTuple.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <boost/program_options.hpp>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/IStorage.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/LimitReadBuffer.h>
#include <IO/WriteHelpers.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Core/ExternalTable.h>
#include <Core/Settings.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <base/scope_guard.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Poco/Net/MessageHeader.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 http_max_multipart_form_data_size;
    extern const SettingsNonZeroUInt64 max_block_size;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

static Block materializeScalar(InputFormatPtr input)
{
    Pipe pipe(std::move(input));
    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    Block block;
    while (block.rows() == 0 && executor.pull(block)) {}
    if (block.rows() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Scalar input returned {} rows", block.rows());

    Block tmp_block;
    while (tmp_block.rows() == 0 && executor.pull(tmp_block)) {}
    if (tmp_block.rows() > 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Scalar input returned more than one block");

    if (block.columns() == 1)
        return block;

    return Block(ColumnsWithTypeAndName{{
        ColumnTuple::create(block.getColumns()),
        std::make_shared<DataTypeTuple>(block.getDataTypes(), block.getNames()),
        "tuple"
    }});
}

ExternalTableDataPtr BaseExternalTable::getData(ContextPtr context)
{
    initReadBuffer();
    initSampleBlock();
    auto input = context->getInputFormat(format, *read_buffer, sample_block, context->getSettingsRef()[Setting::max_block_size]);

    auto data = std::make_unique<ExternalTableData>();
    data->pipe = std::make_unique<QueryPipelineBuilder>();
    data->table_name = name;
    data->pipe->init(Pipe(std::move(input)));

    return data;
}

Block BaseExternalTable::getScalar(ContextPtr context)
{
    initReadBuffer();
    initSampleBlock();
    auto input = context->getInputFormat(format, *read_buffer, sample_block, context->getSettingsRef()[Setting::max_block_size]);
    return materializeScalar(std::move(input));
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
    ParserNameTypePairList parser;
    const auto * pos = argument.data();
    String error;
    ASTPtr columns_list_raw = tryParseQuery(parser, pos, pos + argument.size(), error, false, "", false, DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS, true);

    if (!columns_list_raw)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while parsing table structure: {}", error);

    for (auto & child : columns_list_raw->children)
    {
        auto * column = child->as<ASTNameTypePair>();
        /// We use `formatWithPossiblyHidingSensitiveData` instead of `getColumnNameWithoutAlias` because `column->type` is an ASTFunction.
        /// `getColumnNameWithoutAlias` will return name of the function with `(arguments)` even if arguments is empty.
        if (column)
            structure.emplace_back(
                column->name,
                column->type->formatWithPossiblyHidingSensitiveData(
                    /*max_length=*/0,
                    /*one_line=*/true,
                    /*show_secrets=*/true,
                    /*print_pretty_type_names=*/false,
                    /*identifier_quoting_rule=*/IdentifierQuotingRule::WhenNecessary,
                    /*identifier_quoting_style=*/IdentifierQuotingStyle::Backticks));
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while parsing table structure: expected column definition, got {}", child->formatForErrorMessage());
    }
}

void BaseExternalTable::parseStructureFromTypesField(const std::string & argument)
{
    ParserTypeList parser;
    const auto * pos = argument.data();
    String error;
    ASTPtr type_list_raw = tryParseQuery(parser, pos, pos+argument.size(), error, false, "", false, DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS, true);

    if (!type_list_raw)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while parsing table structure: {}", error);

    for (size_t i = 0; i < type_list_raw->children.size(); ++i)
        structure.emplace_back(
            "_" + toString(i + 1),
            type_list_raw->children[i]->formatWithPossiblyHidingSensitiveData(
                /*max_length=*/0,
                /*one_line=*/true,
                /*show_secrets=*/true,
                /*print_pretty_type_names=*/false,
                /*identifier_quoting_rule=*/IdentifierQuotingRule::WhenNecessary,
                /*identifier_quoting_style=*/IdentifierQuotingStyle::Backticks));
}

void BaseExternalTable::initSampleBlock()
{
    if (!sample_block.empty())
        return;

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
    if (external_options.contains("file"))
        file = external_options["file"].as<std::string>();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "--file field have not been provided for external table");

    if (external_options.contains("name"))
        name = external_options["name"].as<std::string>();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "--name field have not been provided for external table");

    if (external_options.contains("format"))
        format = external_options["format"].as<std::string>();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "--format field have not been provided for external table");

    if (external_options.contains("structure"))
        parseStructureFromStructureField(external_options["structure"].as<std::string>());
    else if (external_options.contains("types"))
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

    const size_t form_data_size_limit = settings[Setting::http_max_multipart_form_data_size];
    if (form_data_size_limit)
        read_buffer = std::make_unique<LimitReadBuffer>(
            stream,
            LimitReadBuffer::Settings{
                .read_no_more = form_data_size_limit > form_data_bytes_read ? form_data_size_limit - form_data_bytes_read : 0,
                .expect_eof = true,
                .excetion_hint = "the maximum size of multipart/form-data. This limit can be tuned by 'http_max_multipart_form_data_size' setting",
            });
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

    auto temporary_id = StorageID::createEmpty();
    temporary_id.table_name = data->table_name;

    auto resolved = getContext()->tryResolveStorageID(temporary_id, Context::ResolveExternal);

    StoragePtr storage;
    if (resolved)
    {
        LOG_TEST(getLogger("ExternalTablesHandler"), "Using existing table {} for external data", temporary_id.getNameForLogs());
        storage = DatabaseCatalog::instance().getTable(resolved, getContext());
    }
    else
    {
        LOG_TEST(getLogger("ExternalTablesHandler"), "Creating temporary table {} for external data", temporary_id.getNameForLogs());
        NamesAndTypesList columns = sample_block.getNamesAndTypesList();
        auto temporary_table = TemporaryTableHolder(getContext(), ColumnsDescription{columns}, {});
        storage = temporary_table.getTable();
        getContext()->addExternalTable(temporary_id.table_name, std::move(temporary_table));
    }

    const auto metadata_snapshot = storage->getInMemoryMetadataPtr(getContext(), false);

    /// The schema of an external table is bound once, by the first part that names it (see the branch above),
    /// and the `_structure` / `_types` fields of every later part with the same name must describe that same
    /// schema. The input format parses the part with the schema its own fields declare, and the columns then
    /// reach the table as a `Chunk`, which carries no types at all: `MemorySink::consume` labels them with the
    /// table header again. A part declaring other types would therefore not be rejected anywhere, and its data
    /// would later be read as the type the header names - a type confusion on data the client controls.
    if (resolved && !isCompatibleHeader(sample_block, metadata_snapshot->getSampleBlock()))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Structure of the data for external table {} does not match the structure of the table. "
            "Received:\n{}\nExpected:\n{}",
            backQuoteIfNeed(temporary_id.table_name),
            sample_block.dumpStructure(),
            metadata_snapshot->getSampleBlock().dumpStructure());

    auto sink = storage->write(ASTPtr(), metadata_snapshot, getContext(), /*async_insert=*/false);

    /// Write data
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*data->pipe));
    pipeline.complete(std::move(sink));
    pipeline.setNumThreads(1);

    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    /// The limiter checks `expect_eof` in `nextImpl`, which a format that stopped exactly at the
    /// budget never reached. Ask it, so the check runs whatever the format did with the part.
    if (form_data_size_limit)
        read_buffer->eof();

    form_data_bytes_read += read_buffer->count();
}

}
