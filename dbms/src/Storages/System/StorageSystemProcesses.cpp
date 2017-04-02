#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/ProcessList.h>
#include <Storages/System/StorageSystemProcesses.h>
#include <Interpreters/Context.h>


namespace DB
{


StorageSystemProcesses::StorageSystemProcesses(const std::string & name_)
    : name(name_)
    , columns{
        { "is_initial_query",     std::make_shared<DataTypeUInt8>() },

        { "user",                 std::make_shared<DataTypeString>() },
        { "query_id",             std::make_shared<DataTypeString>() },
        { "address",             std::make_shared<DataTypeString>() },
        { "port",                 std::make_shared<DataTypeUInt16>() },

        { "initial_user",         std::make_shared<DataTypeString>() },
        { "initial_query_id",     std::make_shared<DataTypeString>() },
        { "initial_address",     std::make_shared<DataTypeString>() },
        { "initial_port",         std::make_shared<DataTypeUInt16>() },

        { "interface",            std::make_shared<DataTypeUInt8>() },

        { "os_user",                std::make_shared<DataTypeString>() },
        { "client_hostname",        std::make_shared<DataTypeString>() },
        { "client_name",            std::make_shared<DataTypeString>() },
        { "client_version_major",    std::make_shared<DataTypeUInt64>() },
        { "client_version_minor",    std::make_shared<DataTypeUInt64>() },
        { "client_revision",        std::make_shared<DataTypeUInt64>() },

        { "http_method",            std::make_shared<DataTypeUInt8>() },
        { "http_user_agent",        std::make_shared<DataTypeString>() },

        { "quota_key",            std::make_shared<DataTypeString>() },

        { "elapsed",             std::make_shared<DataTypeFloat64>()    },
        { "read_rows",            std::make_shared<DataTypeUInt64>()    },
        { "read_bytes",            std::make_shared<DataTypeUInt64>()    },
        { "total_rows_approx",    std::make_shared<DataTypeUInt64>()    },
        { "written_rows",        std::make_shared<DataTypeUInt64>()    },
        { "written_bytes",        std::make_shared<DataTypeUInt64>()    },
        { "memory_usage",        std::make_shared<DataTypeInt64>()    },
        { "query",                 std::make_shared<DataTypeString>()    }
    }
{
}

StoragePtr StorageSystemProcesses::create(const std::string & name_)
{
    return make_shared(name_);
}


BlockInputStreams StorageSystemProcesses::read(
    const Names & column_names,
    ASTPtr query,
    const Context & context,
    const Settings & settings,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned threads)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    ProcessList::Info info = context.getProcessList().getInfo();

    Block block = getSampleBlock();

    for (const auto & process : info)
    {
        size_t i = 0;
        block.getByPosition(i++).column->insert(UInt64(process.client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY));
        block.getByPosition(i++).column->insert(process.client_info.current_user);
        block.getByPosition(i++).column->insert(process.client_info.current_query_id);
        block.getByPosition(i++).column->insert(process.client_info.current_address.host().toString());
        block.getByPosition(i++).column->insert(UInt64(process.client_info.current_address.port()));
        block.getByPosition(i++).column->insert(process.client_info.initial_user);
        block.getByPosition(i++).column->insert(process.client_info.initial_query_id);
        block.getByPosition(i++).column->insert(process.client_info.initial_address.host().toString());
        block.getByPosition(i++).column->insert(UInt64(process.client_info.initial_address.port()));
        block.getByPosition(i++).column->insert(UInt64(process.client_info.interface));
        block.getByPosition(i++).column->insert(process.client_info.os_user);
        block.getByPosition(i++).column->insert(process.client_info.client_hostname);
        block.getByPosition(i++).column->insert(process.client_info.client_name);
        block.getByPosition(i++).column->insert(process.client_info.client_version_major);
        block.getByPosition(i++).column->insert(process.client_info.client_version_minor);
        block.getByPosition(i++).column->insert(UInt64(process.client_info.client_revision));
        block.getByPosition(i++).column->insert(UInt64(process.client_info.http_method));
        block.getByPosition(i++).column->insert(process.client_info.http_user_agent);
        block.getByPosition(i++).column->insert(process.client_info.quota_key);
        block.getByPosition(i++).column->insert(process.elapsed_seconds);
        block.getByPosition(i++).column->insert(process.read_rows);
        block.getByPosition(i++).column->insert(process.read_bytes);
        block.getByPosition(i++).column->insert(process.total_rows);
        block.getByPosition(i++).column->insert(process.written_rows);
        block.getByPosition(i++).column->insert(process.written_bytes);
        block.getByPosition(i++).column->insert(process.memory_usage);
        block.getByPosition(i++).column->insert(process.query);
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
