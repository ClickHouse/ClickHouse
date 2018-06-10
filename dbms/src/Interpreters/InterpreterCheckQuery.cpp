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

InterpreterCheckQuery::InterpreterCheckQuery(const ASTPtr & query_ptr_, const Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}


BlockIO InterpreterCheckQuery::execute()
{
    ASTCheckQuery & alter = typeid_cast<ASTCheckQuery &>(*query_ptr);
    String & table_name = alter.table;
    String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;

    StoragePtr table = context.getTable(database_name, table_name);

    auto distributed_table = dynamic_cast<StorageDistributed *>(&*table);
    if (distributed_table != nullptr)
    {
        /// For tables with the Distributed engine, the CHECK TABLE query sends a DESCRIBE TABLE request to all replicas.
        /// The identity of the structures is checked (column names + column types + default types + expressions
        /// by default) of the tables that the distributed table looks at.

        const auto & settings = context.getSettingsRef();

        BlockInputStreams streams = distributed_table->describe(context, settings);
        streams[0] = std::make_shared<UnionBlockInputStream<StreamUnionMode::ExtraInfo>>(
            streams, nullptr, settings.max_distributed_connections);
        streams.resize(1);

        auto stream_ptr = dynamic_cast<IProfilingBlockInputStream *>(&*streams[0]);
        if (stream_ptr == nullptr)
            throw Exception("InterpreterCheckQuery: Internal error", ErrorCodes::LOGICAL_ERROR);
        auto & stream = *stream_ptr;

        /// Get all data from the DESCRIBE TABLE queries.

        TableDescriptions table_descriptions;

        while (true)
        {
            if (stream.isCancelledOrThrowIfKilled())
            {
                BlockIO res;
                res.in = std::make_shared<OneBlockInputStream>(result);
                return res;
            }

            Block block = stream.read();
            if (!block)
                break;

            BlockExtraInfo info = stream.getBlockExtraInfo();
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

        MutableColumnPtr status_column = ColumnUInt8::create();
        MutableColumnPtr host_name_column = ColumnString::create();
        MutableColumnPtr host_address_column = ColumnString::create();
        MutableColumnPtr port_column = ColumnUInt16::create();
        MutableColumnPtr user_column = ColumnString::create();
        MutableColumnPtr structure_class_column = ColumnUInt32::create();
        MutableColumnPtr structure_column = ColumnString::create();

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

        block.insert(ColumnWithTypeAndName(std::move(status_column), std::make_shared<DataTypeUInt8>(), "status"));
        block.insert(ColumnWithTypeAndName(std::move(host_name_column), std::make_shared<DataTypeString>(), "host_name"));
        block.insert(ColumnWithTypeAndName(std::move(host_address_column), std::make_shared<DataTypeString>(), "host_address"));
        block.insert(ColumnWithTypeAndName(std::move(port_column), std::make_shared<DataTypeUInt16>(), "port"));
        block.insert(ColumnWithTypeAndName(std::move(user_column), std::make_shared<DataTypeString>(), "user"));
        block.insert(ColumnWithTypeAndName(std::move(structure_class_column), std::make_shared<DataTypeUInt32>(), "structure_class"));
        block.insert(ColumnWithTypeAndName(std::move(structure_column), std::make_shared<DataTypeString>(), "structure"));

        BlockIO res;
        res.in = std::make_shared<OneBlockInputStream>(block);

        return res;
    }
    else
    {
        auto column = ColumnUInt8::create();
        column->insert(UInt64(table->checkData()));
        result = Block{{ std::move(column), std::make_shared<DataTypeUInt8>(), "result" }};

        BlockIO res;
        res.in = std::make_shared<OneBlockInputStream>(result);

        return res;
    }
}

}
