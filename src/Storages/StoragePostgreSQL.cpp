#include "StoragePostgreSQL.h"

#if USE_LIBPQXX

#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/PostgreSQLBlockInputStream.h>
#include <Core/Settings.h>
#include <Common/parseAddress.h>
#include <Common/assert_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Columns/ColumnNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StoragePostgreSQL::StoragePostgreSQL(
    const StorageID & table_id_,
    const String & remote_table_name_,
    ConnectionPtr connection_,
    const String connection_str_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const Context & context_)
    : IStorage(table_id_)
    , remote_table_name(remote_table_name_)
    , global_context(context_)
    , connection(std::move(connection_))
    , connection_str(connection_str_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
}


Pipe StoragePostgreSQL::read(
    const Names & column_names_,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info_,
    const Context & context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size_,
    unsigned)
{
    metadata_snapshot->check(column_names_, getVirtuals(), getStorageID());

    String query = transformQueryForExternalDatabase(
        query_info_, metadata_snapshot->getColumns().getOrdinary(),
        IdentifierQuotingStyle::DoubleQuotes, "", remote_table_name, context_);

    Block sample_block;
    for (const String & column_name : column_names_)
    {
        auto column_data = metadata_snapshot->getColumns().getPhysical(column_name);
        WhichDataType which(column_data.type);
        if (which.isEnum())
            column_data.type = std::make_shared<DataTypeString>();
        sample_block.insert({ column_data.type, column_data.name });
    }

    checkConnection(connection);
    return Pipe(std::make_shared<SourceFromInputStream>(
            std::make_shared<PostgreSQLBlockInputStream>(connection, query, sample_block, max_block_size_)));
}


BlockOutputStreamPtr StoragePostgreSQL::write(
        const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /* context */)
{
    return std::make_shared<PostgreSQLBlockOutputStream>(*this, metadata_snapshot, connection, remote_table_name);
}


void PostgreSQLBlockOutputStream::writePrefix()
{
    storage.checkConnection(connection);
    work = std::make_unique<pqxx::work>(*connection);
}


void PostgreSQLBlockOutputStream::write(const Block & block)
{
    const auto columns = block.getColumns();
    const size_t num_rows = block.rows(), num_cols = block.columns();
    const auto data_types = block.getDataTypes();

    if (!stream_inserter)
        stream_inserter = std::make_unique<pqxx::stream_to>(*work, remote_table_name, block.getNames());

    /// std::optional lets libpqxx to know if value is NULL
    std::vector<std::optional<std::string>> row(num_cols);

    for (const auto i : ext::range(0, num_rows))
    {
        for (const auto j : ext::range(0, num_cols))
        {
            if (columns[j]->isNullAt(i))
            {
                row[j] = std::nullopt;
            }
            else
            {
                WriteBufferFromOwnString ostr;
                data_types[j]->serializeAsText(*columns[j], i, ostr, FormatSettings{});
                row[j] = std::optional<std::string>(ostr.str());

                if (isArray(data_types[j]))
                {
                    char r;
                    std::replace_if(row[j]->begin(), row[j]->end(), [&](char c)
                    {
                        return ((c == '[') && (r = '{')) || ((c == ']') && (r = '}'));
                    }, r);
                }
            }
        }
        /// pqxx::stream_to is much faster than simple insert, especially for large number of rows
        stream_inserter->write_values(row);
    }
}


void PostgreSQLBlockOutputStream::writeSuffix()
{
    if (stream_inserter)
        stream_inserter->complete();
    work->commit();
}


void StoragePostgreSQL::checkConnection(ConnectionPtr & pg_connection) const
{
    if (!pg_connection->is_open())
    {
        pg_connection->close();
        pg_connection = std::make_shared<pqxx::connection>(connection_str);
    }
}


void registerStoragePostgreSQL(StorageFactory & factory)
{
    factory.registerStorage("PostgreSQL", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 5)
            throw Exception("Storage PostgreSQL requires 5 parameters: "
                            "PostgreSQL('host:port', 'database', 'table', 'username', 'password'.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.local_context);

        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 5432);
        const String & remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();

        String connection_str;
        connection_str = fmt::format("dbname={} host={} port={} user={} password={}",
                engine_args[1]->as<ASTLiteral &>().value.safeGet<String>(),
                parsed_host_port.first, std::to_string(parsed_host_port.second),
                engine_args[3]->as<ASTLiteral &>().value.safeGet<String>(),
                engine_args[4]->as<ASTLiteral &>().value.safeGet<String>());

        auto connection = std::make_shared<pqxx::connection>(connection_str);
        return StoragePostgreSQL::create(
            args.table_id, remote_table, connection, connection_str, args.columns, args.constraints, args.context);
    },
    {
        .source_access_type = AccessType::POSTGRES,
    });
}

}

#endif
