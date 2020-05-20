#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/config_version.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/parseAddress.h>
#include <Processors/Sources/SourceFromInputStream.h>

#include <Storages/RabbitMQ/StorageRabbitMQ.h>
#include <Storages/RabbitMQ/RabbitMQSettings.h>
#include <amqpcpp.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StorageRabbitMQ::StorageRabbitMQ(
        const StorageID & table_id_,
        Context & context_,
        const ColumnsDescription & columns_,
        const String & host_port_,
        const String & routing_key_,
        const String & exchange_name_,
        const String & format_name_,
        char row_delimiter_,
        size_t num_consumers_,
        bool bind_by_id_,
        size_t num_queues_,
        bool hash_exchange_)
        : IStorage(table_id_)
        , global_context(context_.getGlobalContext())
        , rabbitmq_context(Context(global_context))
        , routing_key(global_context.getMacros()->expand(routing_key_))
        , exchange_name(exchange_name_)
        , format_name(global_context.getMacros()->expand(format_name_))
        , row_delimiter(row_delimiter_)
        , num_consumers(num_consumers_)
        , bind_by_id(bind_by_id_)
        , num_queues(num_queues_)
        , hash_exchange(hash_exchange_)
        , log(&Logger::get("StorageRabbitMQ (" + table_id_.table_name + ")"))
        , semaphore(0, num_consumers_)
        , parsed_address(parseAddress(global_context.getMacros()->expand(host_port_), 5672))
{
}


void registerStorageRabbitMQ(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        size_t args_count = engine_args.size();
        bool has_settings = args.storage_def->settings;

        RabbitMQSettings rabbitmq_settings;
        if (has_settings)
        {
            rabbitmq_settings.loadFromQuery(*args.storage_def);
        }

        String host_port = rabbitmq_settings.rabbitmq_host_port;
        if (args_count >= 1)
        {
            const auto * ast = engine_args[0]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::String)
            {
                host_port = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception(String("RabbitMQ host:port must be a string"), ErrorCodes::BAD_ARGUMENTS);
            }
        }

        String routing_key = rabbitmq_settings.rabbitmq_routing_key.value;
        if (args_count >= 2)
        {
            const auto * ast = engine_args[1]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::String)
            {
                routing_key = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception(String("RabbitMQ routing key must be a string"), ErrorCodes::BAD_ARGUMENTS);
            }
        }

        String exchange = rabbitmq_settings.rabbitmq_exchange_name.value;
        if (args_count >= 3)
        {
            engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.local_context);

            const auto * ast = engine_args[2]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::String)
            {
                exchange = safeGet<String>(ast->value);
            }
        }

        String format = rabbitmq_settings.rabbitmq_format.value;
        if (args_count >= 4)
        {
            engine_args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[3], args.local_context);

            const auto * ast = engine_args[3]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::String)
            {
                format = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception("Format must be a string", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        char row_delimiter = rabbitmq_settings.rabbitmq_row_delimiter;
        if (args_count >= 5)
        {
            engine_args[4] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[4], args.local_context);

            const auto * ast = engine_args[4]->as<ASTLiteral>();
            String arg;
            if (ast && ast->value.getType() == Field::Types::String)
            {
                arg = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception("Row delimiter must be a char", ErrorCodes::BAD_ARGUMENTS);
            }
            if (arg.size() > 1)
            {
                throw Exception("Row delimiter must be a char", ErrorCodes::BAD_ARGUMENTS);
            }
            else if (arg.empty())
            {
                row_delimiter = '\0';
            }
            else
            {
                row_delimiter = arg[0];
            }
        }

        size_t bind_by_id = static_cast<bool>(rabbitmq_settings.rabbitmq_bind_by_id);
        if (args_count >= 6)
        {
            const auto * ast = engine_args[5]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                bind_by_id = static_cast<bool>(safeGet<UInt64>(ast->value));
            }
            else
            {
                throw Exception("Hash exchange flag must be a boolean", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        UInt64 num_consumers = rabbitmq_settings.rabbitmq_num_consumers;
        if (args_count >= 7)
        {
            const auto * ast = engine_args[6]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                num_consumers = safeGet<UInt64>(ast->value);
            }
            else
            {
                throw Exception("Number of consumers must be a positive integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        UInt64 num_queues = rabbitmq_settings.rabbitmq_num_queues;
        if (args_count >= 8)
        {
            const auto * ast = engine_args[7]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                num_consumers = safeGet<UInt64>(ast->value);
            }
            else
            {
                throw Exception("Number of queues must be a positive integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        size_t hash_exchange = static_cast<bool>(rabbitmq_settings.rabbitmq_hash_exchange);
        if (args_count >= 9)
        {
            const auto * ast = engine_args[8]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                hash_exchange = static_cast<bool>(safeGet<UInt64>(ast->value));
            }
            else
            {
                throw Exception("Hash exchange flag must be a boolean", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        return StorageRabbitMQ::create(args.table_id, args.context, args.columns, host_port, routing_key, exchange, 
                format, row_delimiter, num_consumers, bind_by_id, num_queues, hash_exchange);
    };

    factory.registerStorage("RabbitMQ", creator_fn, StorageFactory::StorageFeatures{ .supports_settings = true, });

}


NamesAndTypesList StorageRabbitMQ::getVirtuals() const
{
    return NamesAndTypesList{
            {"_exchange", std::make_shared<DataTypeString>()},
            {"_routingKey", std::make_shared<DataTypeString>()}
    };
}

}

