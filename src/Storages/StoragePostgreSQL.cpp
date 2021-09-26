#include "StoragePostgreSQL.h"

#if USE_LIBPQXX
#include <DataStreams/PostgreSQLSource.h>

#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>
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
#include <Core/Settings.h>
#include <Common/parseAddress.h>
#include <Common/assert_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Columns/ColumnNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Common/parseRemoteDescription.h>
#include <Processors/Pipe.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <IO/WriteHelpers.h>
#include <Parsers/getInsertQuery.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}

StoragePostgreSQL::StoragePostgreSQL(
    const StorageID & table_id_,
    postgres::PoolWithFailoverPtr pool_,
    const String & remote_table_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    const String & remote_table_schema_,
    const String & on_conflict_)
    : IStorage(table_id_)
    , remote_table_name(remote_table_name_)
    , remote_table_schema(remote_table_schema_)
    , on_conflict(on_conflict_)
    , pool(std::move(pool_))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}


Pipe StoragePostgreSQL::read(
    const Names & column_names_,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info_,
    ContextPtr context_,
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

    return Pipe(std::make_shared<PostgreSQLSource<>>(pool->get(), query, sample_block, max_block_size_));
}


class PostgreSQLSink : public SinkToStorage
{

using Row = std::vector<std::optional<std::string>>;

public:
    explicit PostgreSQLSink(
        const StorageMetadataPtr & metadata_snapshot_,
        postgres::ConnectionHolderPtr connection_holder_,
        const String & remote_table_name_,
        const String & remote_table_schema_,
        const String & on_conflict_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , metadata_snapshot(metadata_snapshot_)
        , connection_holder(std::move(connection_holder_))
        , remote_table_name(remote_table_name_)
        , remote_table_schema(remote_table_schema_)
        , on_conflict(on_conflict_)
    {
    }

    String getName() const override { return "PostgreSQLSink"; }

    void consume(Chunk chunk) override
    {
        auto block = getPort().getHeader().cloneWithColumns(chunk.detachColumns());

        if (!inserter)
        {
            if (on_conflict.empty())
            {
                inserter = std::make_unique<StreamTo>(connection_holder->get(),
                        remote_table_schema.empty() ? pqxx::table_path({remote_table_name})
                                                    : pqxx::table_path({remote_table_schema, remote_table_name}), block.getNames());
            }
            else
            {
                inserter = std::make_unique<PreparedInsert>(connection_holder->get(), remote_table_name,
                                                            remote_table_schema, block.getColumnsWithTypeAndName(), on_conflict);
            }
        }

        const auto columns = block.getColumns();
        const size_t num_rows = block.rows(), num_cols = block.columns();
        const auto data_types = block.getDataTypes();

        /// std::optional lets libpqxx to know if value is NULL
        std::vector<std::optional<std::string>> row(num_cols);

        for (const auto i : collections::range(0, num_rows))
        {
            for (const auto j : collections::range(0, num_cols))
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
                        data_types[j]->getDefaultSerialization()->serializeText(*columns[j], i, ostr, FormatSettings{});
                    }

                    row[j] = ostr.str();
                }
            }

            inserter->insert(row);
        }
    }

    void onFinish() override
    {
        if (inserter)
            inserter->complete();
    }

    /// Cannot just use serializeAsText for array data type even though it converts perfectly
    /// any dimension number array into text format, because it encloses in '[]' and for postgres it must be '{}'.
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
        data_type->getDefaultSerialization()->serializeText(*array_column, 0, ostr, FormatSettings{});

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
        else if (which.isDateTime64())
        {
            nested_column = ColumnDecimal<DateTime64>::create(0, 6);
        }
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
    struct Inserter
    {
        pqxx::connection & connection;
        pqxx::work tx;

        explicit Inserter(pqxx::connection & connection_)
            : connection(connection_)
            , tx(connection) {}

        virtual ~Inserter() = default;

        virtual void insert(const Row & row) = 0;
        virtual void complete() = 0;
    };

    struct StreamTo : Inserter
    {
        Names columns;
        pqxx::stream_to stream;

        StreamTo(pqxx::connection & connection_, pqxx::table_path table_, Names columns_)
            : Inserter(connection_)
            , columns(std::move(columns_))
            , stream(pqxx::stream_to::raw_table(tx, connection.quote_table(table_), connection.quote_columns(columns)))
        {
        }

        void complete() override
        {
            stream.complete();
            tx.commit();
        }

        void insert(const Row & row) override
        {
            stream.write_values(row);
        }
    };

    struct PreparedInsert : Inserter
    {
        PreparedInsert(pqxx::connection & connection_, const String & table, const String & schema,
                       const ColumnsWithTypeAndName & columns, const String & on_conflict_)
            : Inserter(connection_)
        {
            WriteBufferFromOwnString buf;
            buf << getInsertQuery(schema, table, columns, IdentifierQuotingStyle::DoubleQuotes);
            buf << " (";
            for (size_t i = 1; i <= columns.size(); ++i)
            {
                if (i > 1)
                    buf << ", ";
                buf << "$" << i;
            }
            buf << ") ";
            buf << on_conflict_;
            connection.prepare("insert", buf.str());
        }

        void complete() override
        {
            connection.unprepare("insert");
            tx.commit();
        }

        void insert(const Row & row) override
        {
            pqxx::params params;
            params.reserve(row.size());
            params.append_multi(row);
            tx.exec_prepared("insert", params);
        }
    };

    StorageMetadataPtr metadata_snapshot;
    postgres::ConnectionHolderPtr connection_holder;
    const String remote_db_name, remote_table_name, remote_table_schema, on_conflict;

    std::unique_ptr<Inserter> inserter;
};


SinkToStoragePtr StoragePostgreSQL::write(
        const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /* context */)
{
    return std::make_shared<PostgreSQLSink>(metadata_snapshot, pool->get(), remote_table_name, remote_table_schema, on_conflict);
}


void registerStoragePostgreSQL(StorageFactory & factory)
{
    factory.registerStorage("PostgreSQL", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() < 5 || engine_args.size() > 7)
            throw Exception("Storage PostgreSQL requires from 5 to 7 parameters: "
                            "PostgreSQL('host:port', 'database', 'table', 'username', 'password' [, 'schema', 'ON CONFLICT ...']",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

        auto host_port = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        /// Split into replicas if needed.
        size_t max_addresses = args.getContext()->getSettingsRef().glob_expansion_max_elements;
        auto addresses = parseRemoteDescriptionForExternalDatabase(host_port, max_addresses, 5432);

        const String & remote_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        const String & username = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
        const String & password = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();

        String remote_table_schema, on_conflict;
        if (engine_args.size() >= 6)
            remote_table_schema = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();
        if (engine_args.size() >= 7)
            on_conflict = engine_args[6]->as<ASTLiteral &>().value.safeGet<String>();

        auto pool = std::make_shared<postgres::PoolWithFailover>(
            remote_database,
            addresses,
            username,
            password,
            args.getContext()->getSettingsRef().postgresql_connection_pool_size,
            args.getContext()->getSettingsRef().postgresql_connection_pool_wait_timeout);

        return StoragePostgreSQL::create(
            args.table_id,
            std::move(pool),
            remote_table,
            args.columns,
            args.constraints,
            args.comment,
            remote_table_schema,
            on_conflict);
    },
    {
        .source_access_type = AccessType::POSTGRES,
    });
}

}

#endif
