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
#include <Core/Settings.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <base/scope_guard.h>
#include <Poco/Net/MessageHeader.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 http_max_multipart_form_data_size;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Parsing a list of types with `,` as separator. For example, `Int, Enum('foo'=1,'bar'=2), Double`
/// Used in `parseStructureFromTypesField`
class ParserTypeList : public IParserBase
{
protected:
    const char * getName() const override { return "type pair list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        return ParserList(std::make_unique<ParserDataType>(), std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
    }
};

ExternalTableDataPtr BaseExternalTable::getData(ContextPtr context)
{
    initReadBuffer();
    initSampleBlock();
    auto input = context->getInputFormat(format, *read_buffer, sample_block, context->getSettingsRef().get("max_block_size").safeGet<UInt64>());

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
    if (sample_block)
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

    if (settings[Setting::http_max_multipart_form_data_size])
        read_buffer = std::make_unique<LimitReadBuffer>(
            stream,
            settings[Setting::http_max_multipart_form_data_size],
            /* trow_exception */ true,
            /* exact_limit */ std::optional<size_t>(),
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
