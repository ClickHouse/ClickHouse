#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCheckQuery.h>
#include <Parsers/ASTCheckQuery.h>
#include <Storages/StorageDistributed.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>

#include <openssl/sha.h>
#include <deque>
#include <array>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Interpreters/ClusterProxy/DescribeStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_BLOCK_EXTRA_INFO;
    extern const int RECEIVED_EMPTY_DATA;
}


namespace
{

/// A helper structure for performing a response to a DESCRIBE TABLE query with a Distributed table.
/// Contains information about the local table that was retrieved from a single replica.
struct TableDescription
{
    TableDescription(const Block & block, const BlockExtraInfo & extra_info_)
        : extra_info(extra_info_)
    {
        const auto & name_column = typeid_cast<const ColumnString &>(*block.getByName("name").column);
        const auto & type_column = typeid_cast<const ColumnString &>(*block.getByName("type").column);
        const auto & default_type_column = typeid_cast<const ColumnString &>(*block.getByName("default_type").column);
        const auto & default_expression_column = typeid_cast<const ColumnString &>(*block.getByName("default_expression").column);

        size_t row_count = block.rows();

        names_with_types.reserve(name_column.byteSize() + type_column.byteSize() + (3 * row_count));

        SHA512_CTX ctx;
        SHA512_Init(&ctx);

        bool is_first = true;
        for (size_t i = 0; i < row_count; ++i)
        {
            const auto & name = name_column.getDataAt(i).toString();
            const auto & type = type_column.getDataAt(i).toString();
            const auto & default_type = default_type_column.getDataAt(i).toString();
            const auto & default_expression = default_expression_column.getDataAt(i).toString();

            names_with_types.append(is_first ? "" : ", ");
            names_with_types.append(name);
            names_with_types.append(" ");
            names_with_types.append(type);

            SHA512_Update(&ctx, reinterpret_cast<const unsigned char *>(name.data()), name.size());
            SHA512_Update(&ctx, reinterpret_cast<const unsigned char *>(type.data()), type.size());
            SHA512_Update(&ctx, reinterpret_cast<const unsigned char *>(default_type.data()), default_type.size());
            SHA512_Update(&ctx, reinterpret_cast<const unsigned char *>(default_expression.data()), default_expression.size());

            is_first = false;
        }

        SHA512_Final(hash.data(), &ctx);
    }

    using Hash = std::array<unsigned char, SHA512_DIGEST_LENGTH>;

    BlockExtraInfo extra_info;
    std::string names_with_types;
    Hash hash;
    UInt32 structure_class;
};

inline bool operator<(const TableDescription & lhs, const TableDescription & rhs)
{
    return lhs.hash < rhs.hash;
}

using TableDescriptions = std::deque<TableDescription>;

}

InterpreterCheckQuery::InterpreterCheckQuery(const ASTPtr & query_ptr_with_local_table_, const Context & context_)
    : query_ptr(query_ptr_with_local_table_), context(context_)
{
}


InterpreterCheckQuery::InterpreterCheckQuery(RemoteTablesInfo remote_tables_, const Context & context_)
: remote_tables(std::move(remote_tables_)), context(context_) {}


Block InterpreterCheckQuery::getSampleBlock() const
{
    Block block;
    ColumnWithTypeAndName col;

    col.name = "status";
    col.type = std::make_shared<DataTypeUInt8>();
    col.column = col.type->createColumn();
    block.insert(col);

    col.name = "host_name";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    block.insert(col);

    col.name = "host_address";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    block.insert(col);

    col.name = "port";
    col.type = std::make_shared<DataTypeUInt16>();
    col.column = col.type->createColumn();
    block.insert(col);

    col.name = "user";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    block.insert(col);

    col.name = "structure_class";
    col.type = std::make_shared<DataTypeUInt32>();
    col.column = col.type->createColumn();
    block.insert(col);

    col.name = "structure";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    block.insert(col);

    return block;
}

static BlockInputStreams getQueryDescribeStreams(const InterpreterCheckQuery::RemoteTablesInfo & info, const Context & context,
                                                 const Settings & settings)
{
    /// Create DESCRIBE TABLE query.

    ASTPtr describe_query_ptr = std::make_shared<ASTDescribeQuery>();
    auto & describe_query = static_cast<ASTDescribeQuery &>(*describe_query_ptr);

    describe_query.database = info.remote_database;
    describe_query.table = info.remote_table;

    ClusterProxy::DescribeStreamFactory describe_stream_factory;

    return ClusterProxy::executeQuery(
            describe_stream_factory, info.cluster, describe_query_ptr, context, settings);
}


BlockIO InterpreterCheckQuery::execute()
{
    if (query_ptr)
    {
        /// Ordinary case. We have local instance of Distributed table and will check its remote (i.e. destination) tables
        ASTCheckQuery & alter = typeid_cast<ASTCheckQuery &>(*query_ptr);
        String & table_name = alter.table;
        String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;

        StoragePtr table = context.getTable(database_name, table_name);
        auto distributed_table = typeid_cast<StorageDistributed *>(&*table);

        /// Fallback, it is not Distributed table
        if (!distributed_table)
        {
            result = Block{{ std::make_shared<ColumnUInt8>(), std::make_shared<DataTypeUInt8>(), "result" }};
            result.safeGetByPosition(0).column->insert(Field(UInt64(table->checkData())));

            BlockIO res;
            res.in = std::make_shared<OneBlockInputStream>(result);
            res.in_sample = result.cloneEmpty();

            return res;
        }

        // Using of distributed_table->getCluster() is bad idea
        remote_tables.cluster = context.getCluster(distributed_table->getClusterName());
        remote_tables.remote_database = distributed_table->getRemoteDatabaseName();
        remote_tables.remote_table = distributed_table->getRemoteTableName();
    }
    else
    {
        /// Specific case. We don't have local instance of Distributed table, but want to know structure of remote tables

        if (remote_tables.cluster == nullptr)
            throw Exception("There is no cluster", ErrorCodes::LOGICAL_ERROR);
    }

    /// For tables with the Distributed engine, the CHECK TABLE query sends a DESCRIBE TABLE request to all replicas.
    /// The identity of the structures is checked (column names + column types + default types + expressions
    /// by default) of the tables that the distributed table looks at.

    const auto & settings = context.getSettingsRef();

    auto stream = std::make_shared<UnionBlockInputStream<StreamUnionMode::ExtraInfo>>(
        getQueryDescribeStreams(remote_tables, context, settings), nullptr, settings.max_distributed_connections);

    auto stream_ptr = dynamic_cast<IProfilingBlockInputStream *>(stream.get());
    if (stream_ptr == nullptr)
        throw Exception("InterpreterCheckQuery: Internal error", ErrorCodes::LOGICAL_ERROR);

    /// Get all data from the DESCRIBE TABLE queries.

    TableDescriptions table_descriptions;

    while (true)
    {
        if (stream_ptr->isCancelled())
        {
            BlockIO res;
            res.in = std::make_shared<OneBlockInputStream>(result);
            return res;
        }

        Block block = stream_ptr->read();
        if (!block)
            break;

        BlockExtraInfo info = stream_ptr->getBlockExtraInfo();
        if (!info.is_valid)
            throw Exception("Received invalid block extra info", ErrorCodes::INVALID_BLOCK_EXTRA_INFO);

        table_descriptions.emplace_back(block, info);
    }

    if (table_descriptions.empty())
        throw Exception("Received empty data", ErrorCodes::RECEIVED_EMPTY_DATA);

    /// Define an equivalence class for each table structure.

    std::sort(table_descriptions.begin(), table_descriptions.end());

    UInt32 structure_class = 0;

    auto it = table_descriptions.begin();
    it->structure_class = structure_class;

    auto prev = it;
    for (++it; it != table_descriptions.end(); ++it)
    {
        if (*prev < *it)
            ++structure_class;
        it->structure_class = structure_class;
        prev = it;
    }

    /// Construct the result.

    ColumnPtr status_column = std::make_shared<ColumnUInt8>();
    ColumnPtr host_name_column = std::make_shared<ColumnString>();
    ColumnPtr host_address_column = std::make_shared<ColumnString>();
    ColumnPtr port_column = std::make_shared<ColumnUInt16>();
    ColumnPtr user_column = std::make_shared<ColumnString>();
    ColumnPtr structure_class_column = std::make_shared<ColumnUInt32>();
    ColumnPtr structure_column = std::make_shared<ColumnString>();

    /// This value is 1 if the structure is not disposed of anywhere, but 0 otherwise.
    UInt8 status_value = (structure_class == 0) ? 1 : 0;

    for (const auto & desc : table_descriptions)
    {
        status_column->insert(static_cast<UInt64>(status_value));
        structure_class_column->insert(static_cast<UInt64>(desc.structure_class));
        host_name_column->insert(desc.extra_info.host);
        host_address_column->insert(desc.extra_info.resolved_address);
        port_column->insert(static_cast<UInt64>(desc.extra_info.port));
        user_column->insert(desc.extra_info.user);
        structure_column->insert(desc.names_with_types);
    }

    Block block;

    block.insert(ColumnWithTypeAndName(status_column, std::make_shared<DataTypeUInt8>(), "status"));
    block.insert(ColumnWithTypeAndName(host_name_column, std::make_shared<DataTypeString>(), "host_name"));
    block.insert(ColumnWithTypeAndName(host_address_column, std::make_shared<DataTypeString>(), "host_address"));
    block.insert(ColumnWithTypeAndName(port_column, std::make_shared<DataTypeUInt16>(), "port"));
    block.insert(ColumnWithTypeAndName(user_column, std::make_shared<DataTypeString>(), "user"));
    block.insert(ColumnWithTypeAndName(structure_class_column, std::make_shared<DataTypeUInt32>(), "structure_class"));
    block.insert(ColumnWithTypeAndName(structure_column, std::make_shared<DataTypeString>(), "structure"));

    BlockIO res;
    res.in = std::make_shared<OneBlockInputStream>(block);
    res.in_sample = getSampleBlock();

    return res;
}


}
