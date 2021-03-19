#include "StoragePostgreSQL.h"

#if USE_LIBPQXX

#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/PostgreSQL/PostgreSQLConnection.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
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
    extern const int NOT_IMPLEMENTED;
}

StoragePostgreSQL::StoragePostgreSQL(
    const StorageID & table_id_,
    const String & remote_table_name_,
    PostgreSQLConnectionPoolPtr connection_pool_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const Context & context_,
    const String & remote_table_schema_)
    : IStorage(table_id_)
    , remote_table_name(remote_table_name_)
    , remote_table_schema(remote_table_schema_)
    , global_context(context_)
    , connection_pool(std::move(connection_pool_))
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

    /// Connection is already made to the needed database, so it should not be present in the query;
    /// remote_table_schema is empty if it is not specified, will access only table_name.
    String query = transformQueryForExternalDatabase(
        query_info_, metadata_snapshot->getColumns().getOrdinary(),
        IdentifierQuotingStyle::DoubleQuotes, remote_table_schema, remote_table_name, context_);

    Block sample_block;
    for (const String & column_name : column_names_)
    {
        auto column_data = metadata_snapshot->getColumns().getPhysical(column_name);
        WhichDataType which(column_data.type);
        if (which.isEnum())
            column_data.type = std::make_shared<DataTypeString>();
        sample_block.insert({ column_data.type, column_data.name });
    }

    return Pipe(std::make_shared<SourceFromInputStream>(
            std::make_shared<PostgreSQLBlockInputStream>(connection_pool->get(), query, sample_block, max_block_size_)));
}


class PostgreSQLBlockOutputStream : public IBlockOutputStream
{
public:
    explicit PostgreSQLBlockOutputStream(
        const StorageMetadataPtr & metadata_snapshot_,
        PostgreSQLConnectionHolderPtr connection_,
        const std::string & remote_table_name_)
        : metadata_snapshot(metadata_snapshot_)
        , connection(std::move(connection_))
        , remote_table_name(remote_table_name_)
    {
    }

    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }


    void writePrefix() override
    {
        work = std::make_unique<pqxx::work>(connection->conn());
    }


    void write(const Block & block) override
    {
        if (!work)
            return;

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

                    if (isArray(data_types[j]))
                    {
                        parseArray((*columns[j])[i], data_types[j], ostr);
                    }
                    else
                    {
                        data_types[j]->serializeAsText(*columns[j], i, ostr, FormatSettings{});
                    }

                    row[j] = ostr.str();
                }
            }

            stream_inserter->write_values(row);
        }
    }


    void writeSuffix() override
    {
        if (stream_inserter)
        {
            stream_inserter->complete();
            work->commit();
        }
    }


    /// Cannot just use serializeAsText for array data type even though it converts perfectly
    /// any dimension number array into text format, because it incloses in '[]' and for postgres it must be '{}'.
    /// Check if array[...] syntax from PostgreSQL will be applicable.
    void parseArray(const Field & array_field, const DataTypePtr & data_type, WriteBuffer & ostr)
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(data_type.get());
        const auto & nested = array_type->getNestedType();
        const auto & array = array_field.get<Array>();

        if (!isArray(nested))
        {
            writeText(clickhouseToPostgresArray(array, data_type), ostr);
            return;
        }

        writeChar('{', ostr);

        const auto * nested_array_type = typeid_cast<const DataTypeArray *>(nested.get());
        for (auto iter = array.begin(); iter != array.end(); ++iter)
        {
            if (iter != array.begin())
                writeText(", ", ostr);

            if (!isArray(nested_array_type->getNestedType()))
            {
                writeText(clickhouseToPostgresArray(iter->get<Array>(), nested), ostr);
            }
            else
            {
                parseArray(*iter, nested, ostr);
            }
        }

        writeChar('}', ostr);
    }


    /// Conversion is done via column casting because with writeText(Array..) got incorrect conversion
    /// of Date and DateTime data types and it added extra quotes for values inside array.
    static std::string clickhouseToPostgresArray(const Array & array_field, const DataTypePtr & data_type)
    {
        auto nested = typeid_cast<const DataTypeArray *>(data_type.get())->getNestedType();
        auto array_column = ColumnArray::create(createNested(nested));
        array_column->insert(array_field);
        WriteBufferFromOwnString ostr;
        data_type->serializeAsText(*array_column, 0, ostr, FormatSettings{});

        /// ostr is guaranteed to be at least '[]', i.e. size is at least 2 and 2 only if ostr.str() == '[]'
        assert(ostr.str().size() >= 2);
        return '{' + std::string(ostr.str().begin() + 1, ostr.str().end() - 1) + '}';
    }


    static MutableColumnPtr createNested(DataTypePtr nested)
    {
        bool is_nullable = false;
        if (nested->isNullable())
        {
            is_nullable = true;
            nested = static_cast<const DataTypeNullable *>(nested.get())->getNestedType();
        }

        WhichDataType which(nested);
        MutableColumnPtr nested_column;
        if (which.isString() || which.isFixedString())   nested_column = ColumnString::create();
        else if (which.isInt8() || which.isInt16())      nested_column = ColumnInt16::create();
        else if (which.isUInt8() || which.isUInt16())    nested_column = ColumnUInt16::create();
        else if (which.isInt32())                        nested_column = ColumnInt32::create();
        else if (which.isUInt32())                       nested_column = ColumnUInt32::create();
        else if (which.isInt64())                        nested_column = ColumnInt64::create();
        else if (which.isUInt64())                       nested_column = ColumnUInt64::create();
        else if (which.isFloat32())                      nested_column = ColumnFloat32::create();
        else if (which.isFloat64())                      nested_column = ColumnFloat64::create();
        else if (which.isDate())                         nested_column = ColumnUInt16::create();
        else if (which.isDateTime())                     nested_column = ColumnUInt32::create();
        else if (which.isDecimal32())
        {
            const auto & type = typeid_cast<const DataTypeDecimal<Decimal32> *>(nested.get());
            nested_column = ColumnDecimal<Decimal32>::create(0, type->getScale());
        }
        else if (which.isDecimal64())
        {
            const auto & type = typeid_cast<const DataTypeDecimal<Decimal64> *>(nested.get());
            nested_column = ColumnDecimal<Decimal64>::create(0, type->getScale());
        }
        else if (which.isDecimal128())
        {
            const auto & type = typeid_cast<const DataTypeDecimal<Decimal128> *>(nested.get());
            nested_column = ColumnDecimal<Decimal128>::create(0, type->getScale());
        }
        else if (which.isDecimal256())
        {
            const auto & type = typeid_cast<const DataTypeDecimal<Decimal256> *>(nested.get());
            nested_column = ColumnDecimal<Decimal256>::create(0, type->getScale());
        }
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Type conversion not supported");

        if (is_nullable)
            return ColumnNullable::create(std::move(nested_column), ColumnUInt8::create(nested_column->size(), 0));

        return nested_column;
    }


private:
    StorageMetadataPtr metadata_snapshot;
    PostgreSQLConnectionHolderPtr connection;
    std::string remote_table_name;

    std::unique_ptr<pqxx::work> work;
    std::unique_ptr<pqxx::stream_to> stream_inserter;
};


BlockOutputStreamPtr StoragePostgreSQL::write(
        const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /* context */)
{
    return std::make_shared<PostgreSQLBlockOutputStream>(metadata_snapshot, connection_pool->get(), remote_table_name);
}


void registerStoragePostgreSQL(StorageFactory & factory)
{
    factory.registerStorage("PostgreSQL", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() < 5 || engine_args.size() > 6)
            throw Exception("Storage PostgreSQL requires from 5 to 6 parameters: "
                            "PostgreSQL('host:port', 'database', 'table', 'username', 'password' [, 'schema']",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.local_context);

        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 5432);
        const String & remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();

        String remote_table_schema;
        if (engine_args.size() == 6)
            remote_table_schema = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();

        auto connection_pool = std::make_shared<PostgreSQLConnectionPool>(
            engine_args[1]->as<ASTLiteral &>().value.safeGet<String>(),
            parsed_host_port.first,
            parsed_host_port.second,
            engine_args[3]->as<ASTLiteral &>().value.safeGet<String>(),
            engine_args[4]->as<ASTLiteral &>().value.safeGet<String>(),
            args.context.getSettingsRef().postgresql_connection_pool_size,
            args.context.getSettingsRef().postgresql_connection_pool_wait_timeout);

        return StoragePostgreSQL::create(
            args.table_id, remote_table, connection_pool, args.columns, args.constraints, args.context, remote_table_schema);
    },
    {
        .source_access_type = AccessType::POSTGRES,
    });
}

}

#endif
