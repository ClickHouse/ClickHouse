#include "config.h"
#if USE_MYSQL

#if __has_include(<mysql.h>)
#include <mysql.h>
#else
#include <mysql/mysql.h>
#endif

// NB: swapping mysql.h include with unit's header breaks the build because of enum forward declaration in mysqlxx/Types.h
// See another example of such issue in mysqlxx/Row.cpp
#include "StorageMySQLSelect.h"

#include <Common/logger_useful.h>
#include <Common/parseRemoteDescription.h>
#include <Common/quoteString.h>
#include <Common/RemoteHostFilter.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/MySQLSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/MySQL/MySQLHelpers.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/StorageFactory.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/algorithm/transform.hpp>

#include <memory>


namespace DB
{

namespace Setting
{
extern const SettingsUInt64 glob_expansion_max_elements;
extern const SettingsMySQLDataTypesSupport mysql_datatypes_support_level;
}

namespace MySQLSetting
{
extern const MySQLSettingsBool connection_auto_close;
}

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StorageMySQLSelect::StorageMySQLSelect(
    const StorageID & table_id_,
    mysqlxx::PoolWithFailover && pool_,
    const std::string & remote_database_name_,
    const std::string select_query_,
    const ColumnsDescription & columns_,
    ContextPtr context_,
    const MySQLSettings & mysql_settings_)

    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , remote_database_name(remote_database_name_)
    , select_query(select_query_)
    , mysql_settings(std::make_unique<MySQLSettings>(mysql_settings_))
    , pool(std::make_shared<mysqlxx::PoolWithFailover>(pool_))
    , log(getLogger("StorageMySQLSelect (" + table_id_.getNameForLogs() + ")"))
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = doQueryResultStructure(pool_, select_query_, context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    setInMemoryMetadata(storage_metadata);
}


Pipe StorageMySQLSelect::read(
    const Names & column_names_,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names_);
    String query_for_columns = "SELECT ";
    for (const auto & column : column_names_)
    {
        query_for_columns += backQuoteMySQL(column) + ", ";
    }
    query_for_columns = query_for_columns.substr(0, query_for_columns.size() - 2);
    query_for_columns += " FROM (" + select_query + ") as t";
    LOG_TRACE(log, "Query: {}", query_for_columns);

    Block sample_block;
    for (const String & column_name : column_names_)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);

        WhichDataType which(column_data.type);
        /// Convert enum to string.
        // if (which.isEnum())
        //     column_data.type = std::make_shared<DataTypeString>();
        sample_block.insert({column_data.type, column_data.name});
    }


    StreamSettings mysql_input_stream_settings(context_->getSettingsRef(), (*mysql_settings)[MySQLSetting::connection_auto_close]);
    return Pipe(std::make_shared<MySQLWithFailoverSource>(pool, query_for_columns, sample_block, mysql_input_stream_settings));
}


StorageMySQLSelect::Configuration StorageMySQLSelect::getConfiguration(ASTs storage_args, ContextPtr context_)
{
    Configuration configuration;
    if (storage_args.size() != 5)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "MySQLSelect storage requires 5 parameters: "
            "'host:port' (or 'addresses_pattern'), database, select query, user, password.");

    for (auto & storage_arg : storage_args)
        storage_arg = evaluateConstantExpressionOrIdentifierAsLiteral(storage_arg, context_);

    configuration.addresses_expr = checkAndGetLiteralArgument<String>(storage_args[0], "host:port");
    size_t max_addresses = context_->getSettingsRef()[Setting::glob_expansion_max_elements];
    configuration.addresses = parseRemoteDescriptionForExternalDatabase(configuration.addresses_expr, max_addresses, 3306);

    configuration.database = checkAndGetLiteralArgument<String>(storage_args[1], "database");
    configuration.select_query = checkAndGetLiteralArgument<String>(storage_args[2], "select_query");
    configuration.username = checkAndGetLiteralArgument<String>(storage_args[3], "username");
    configuration.password = checkAndGetLiteralArgument<String>(storage_args[4], "password");

    for (const auto & address : configuration.addresses)
        context_->getRemoteHostFilter().checkHostAndPort(address.first, toString(address.second));

    return configuration;
}

ColumnsDescription
StorageMySQLSelect::doQueryResultStructure(mysqlxx::PoolWithFailover & pool_, const String & select_query, const ContextPtr & context_)
{
    // const auto & settings = context->getSettingsRef();
    auto limited_select = "(" + select_query + ") LIMIT 0";
    auto conn = pool_.get();
    auto query = conn->query(limited_select);

    auto query_res = query.use();
    auto * fields = query_res.getFields();
    auto field_cnt = query_res.getNumFields();
    if (field_cnt == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MySQL SELECT query returned no fields");


    ColumnsDescription columns;

    const auto & settings = context_->getSettingsRef();
    (void)settings;
    for (size_t i = 0; i < field_cnt; ++i)
    {
        const auto & field = fields[i];

        // Check flags for additional type information
        bool is_nullable = !(field.flags & NOT_NULL_FLAG);
        bool is_unsigned = (field.flags & UNSIGNED_FLAG);

        DataTypePtr data_type;

        switch (field.type)
        {
            case enum_field_types::MYSQL_TYPE_DECIMAL:
                 case enum_field_types::MYSQL_TYPE_NEWDECIMAL:
                {
                    // MySQL DECIMAL includes space for sign and decimal point in length
                    auto precision = field.length;
                    if (field.decimals > 0)
                        precision -= 1; // Decimal point
                    if (!(field.flags & UNSIGNED_FLAG))
                        precision -= 1; // Sign

                    data_type = createDecimal<DataTypeDecimal>(precision, field.decimals);
                }
                break;
            case enum_field_types::MYSQL_TYPE_TINY:
                if (is_unsigned)
                    data_type = std::make_shared<DataTypeUInt8>();
                else
                    data_type = std::make_shared<DataTypeInt8>();
                break;
            case enum_field_types::MYSQL_TYPE_SHORT:
                if (is_unsigned)
                    data_type = std::make_shared<DataTypeUInt16>();
                else
                    data_type = std::make_shared<DataTypeInt16>();
                break;
            case enum_field_types::MYSQL_TYPE_INT24: // Treat int24 as int32
            case enum_field_types::MYSQL_TYPE_LONG:
                if (is_unsigned)
                    data_type = std::make_shared<DataTypeUInt32>();
                else
                    data_type = std::make_shared<DataTypeInt32>();
                break;
            case enum_field_types::MYSQL_TYPE_LONGLONG:
                if (is_unsigned)
                    data_type = std::make_shared<DataTypeUInt64>();
                else
                    data_type = std::make_shared<DataTypeInt64>();
                break;
            case enum_field_types::MYSQL_TYPE_FLOAT:
                data_type = std::make_shared<DataTypeFloat32>();
                break;
            case enum_field_types::MYSQL_TYPE_DOUBLE:
                data_type = std::make_shared<DataTypeFloat64>();
                break;
            case enum_field_types::MYSQL_TYPE_NULL:
                data_type = std::make_shared<DataTypeNothing>();
                break;
            case enum_field_types::MYSQL_TYPE_TIMESTAMP:
                data_type = std::make_shared<DataTypeDateTime>();
                break;
            case enum_field_types::MYSQL_TYPE_DATE:
            case enum_field_types::MYSQL_TYPE_NEWDATE:
                data_type = std::make_shared<DataTypeDate>();
                break;
            case enum_field_types::MYSQL_TYPE_TIME:
                data_type = std::make_shared<DataTypeString>(); // ClickHouse doesn't have a TIME type
                break;
            case enum_field_types::MYSQL_TYPE_DATETIME:
                data_type = std::make_shared<DataTypeDateTime>();
                break;
            case enum_field_types::MYSQL_TYPE_YEAR:
                data_type = std::make_shared<DataTypeUInt16>();
                break;
            case enum_field_types::MYSQL_TYPE_VARCHAR:
                data_type = std::make_shared<DataTypeString>();
                break;
            case enum_field_types::MYSQL_TYPE_BIT:
                data_type = std::make_shared<DataTypeUInt64>();
                break;
            case enum_field_types::MYSQL_TYPE_JSON:
            case enum_field_types::MYSQL_TYPE_ENUM:
            case enum_field_types::MYSQL_TYPE_SET:
            case enum_field_types::MYSQL_TYPE_TINY_BLOB:
            case enum_field_types::MYSQL_TYPE_MEDIUM_BLOB:
            case enum_field_types::MYSQL_TYPE_LONG_BLOB:
            case enum_field_types::MYSQL_TYPE_BLOB:
            case enum_field_types::MYSQL_TYPE_GEOMETRY:
                data_type = std::make_shared<DataTypeString>();
                break;
            default:
                // For unknown types, fallback to String
                data_type = std::make_shared<DataTypeString>();
        }

        if (is_nullable && !data_type->isNullable())
            data_type = makeNullable(data_type);

        ColumnDescription column_description(query_res.getFieldName(i), data_type);
        columns.add(column_description);
    }

    return columns;
}
}


#endif
