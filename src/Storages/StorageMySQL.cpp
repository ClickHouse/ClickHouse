#include "StorageMySQL.h"

#if USE_MYSQL

#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Formats/MySQLBlockInputStream.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Common/parseAddress.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <mysqlxx/Transaction.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

static String backQuoteMySQL(const String & x)
{
    String res(x.size(), '\0');
    {
        WriteBufferFromString wb(res);
        writeBackQuotedStringMySQL(x, wb);
    }
    return res;
}

StorageMySQL::StorageMySQL(
    const StorageID & table_id_,
    mysqlxx::Pool && pool_,
    const std::string & remote_database_name_,
    const std::string & remote_table_name_,
    const bool replace_query_,
    const std::string & on_duplicate_clause_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const Context & context_)
    : IStorage(table_id_)
    , remote_database_name(remote_database_name_)
    , remote_table_name(remote_table_name_)
    , replace_query{replace_query_}
    , on_duplicate_clause{on_duplicate_clause_}
    , pool(std::move(pool_))
    , global_context(context_.getGlobalContext())
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
}


Pipe StorageMySQL::read(
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
        query_info_,
        metadata_snapshot->getColumns().getOrdinary(),
        IdentifierQuotingStyle::BackticksMySQL,
        remote_database_name,
        remote_table_name,
        context_);

    Block sample_block;
    for (const String & column_name : column_names_)
    {
        auto column_data = metadata_snapshot->getColumns().getPhysical(column_name);

        WhichDataType which(column_data.type);
        /// Convert enum to string.
        if (which.isEnum())
            column_data.type = std::make_shared<DataTypeString>();
        sample_block.insert({ column_data.type, column_data.name });
    }

    /// TODO: rewrite MySQLBlockInputStream
    return Pipe(std::make_shared<SourceFromInputStream>(
            std::make_shared<MySQLLazyBlockInputStream>(pool, query, sample_block, max_block_size_, /* auto_close = */ true)));
}


class StorageMySQLBlockOutputStream : public IBlockOutputStream
{
public:
    explicit StorageMySQLBlockOutputStream(
        const StorageMySQL & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::string & remote_database_name_,
        const std::string & remote_table_name_,
        const mysqlxx::PoolWithFailover::Entry & entry_,
        const size_t & mysql_max_rows_to_insert)
        : storage{storage_}
        , metadata_snapshot{metadata_snapshot_}
        , remote_database_name{remote_database_name_}
        , remote_table_name{remote_table_name_}
        , entry{entry_}
        , max_batch_rows{mysql_max_rows_to_insert}
    {
    }

    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }

    void write(const Block & block) override
    {
        auto blocks = splitBlocks(block, max_batch_rows);
        mysqlxx::Transaction trans(entry);
        try
        {
            for (const Block & batch_data : blocks)
            {
                writeBlockData(batch_data);
            }
            trans.commit();
        }
        catch (...)
        {
            trans.rollback();
            throw;
        }
    }

    void writeBlockData(const Block & block)
    {
        WriteBufferFromOwnString sqlbuf;
        sqlbuf << (storage.replace_query ? "REPLACE" : "INSERT") << " INTO ";
        sqlbuf << backQuoteMySQL(remote_database_name) << "." << backQuoteMySQL(remote_table_name);
        sqlbuf << " (" << dumpNamesWithBackQuote(block) << ") VALUES ";

        auto writer = FormatFactory::instance().getOutput("Values", sqlbuf, metadata_snapshot->getSampleBlock(), storage.global_context);
        writer->write(block);

        if (!storage.on_duplicate_clause.empty())
            sqlbuf << " ON DUPLICATE KEY " << storage.on_duplicate_clause;

        sqlbuf << ";";

        auto query = this->entry->query(sqlbuf.str());
        query.execute();
    }

    Blocks splitBlocks(const Block & block, const size_t & max_rows) const
    {
        /// Avoid Excessive copy when block is small enough
        if (block.rows() <= max_rows)
            return Blocks{std::move(block)};

        const size_t splited_block_size = ceil(block.rows() * 1.0 / max_rows);
        Blocks splitted_blocks(splited_block_size);

        for (size_t idx = 0; idx < splited_block_size; ++idx)
            splitted_blocks[idx] = block.cloneEmpty();

        const size_t columns = block.columns();
        const size_t rows = block.rows();
        size_t offsets = 0;
        UInt64 limits = max_batch_rows;
        for (size_t idx = 0; idx < splited_block_size; ++idx)
        {
            /// For last batch, limits should be the remain size
            if (idx == splited_block_size - 1) limits = rows - offsets;
            for (size_t col_idx = 0; col_idx < columns; ++col_idx)
            {
                splitted_blocks[idx].getByPosition(col_idx).column = block.getByPosition(col_idx).column->cut(offsets, limits);
            }
            offsets += max_batch_rows;
        }

        return splitted_blocks;
    }

    static std::string dumpNamesWithBackQuote(const Block & block)
    {
        WriteBufferFromOwnString out;
        for (auto it = block.begin(); it != block.end(); ++it)
        {
            if (it != block.begin())
                out << ", ";
            out << backQuoteMySQL(it->name);
        }
        return out.str();
    }

private:
    const StorageMySQL & storage;
    StorageMetadataPtr metadata_snapshot;
    std::string remote_database_name;
    std::string remote_table_name;
    mysqlxx::PoolWithFailover::Entry entry;
    size_t max_batch_rows;
};


BlockOutputStreamPtr StorageMySQL::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & context)
{
    return std::make_shared<StorageMySQLBlockOutputStream>(*this, metadata_snapshot, remote_database_name, remote_table_name, pool.get(), context.getSettingsRef().mysql_max_rows_to_insert);
}

void registerStorageMySQL(StorageFactory & factory)
{
    factory.registerStorage("MySQL", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() < 5 || engine_args.size() > 7)
            throw Exception(
                "Storage MySQL requires 5-7 parameters: MySQL('host:port', database, table, 'user', 'password'[, replace_query, 'on_duplicate_clause']).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.local_context);

        /// 3306 is the default MySQL port.
        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 3306);

        const String & remote_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        const String & username = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
        const String & password = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();

        mysqlxx::Pool pool(remote_database, parsed_host_port.first, username, password, parsed_host_port.second);

        bool replace_query = false;
        std::string on_duplicate_clause;
        if (engine_args.size() >= 6)
            replace_query = engine_args[5]->as<ASTLiteral &>().value.safeGet<UInt64>();
        if (engine_args.size() == 7)
            on_duplicate_clause = engine_args[6]->as<ASTLiteral &>().value.safeGet<String>();

        if (replace_query && !on_duplicate_clause.empty())
            throw Exception(
                "Only one of 'replace_query' and 'on_duplicate_clause' can be specified, or none of them",
                ErrorCodes::BAD_ARGUMENTS);

        return StorageMySQL::create(
            args.table_id,
            std::move(pool),
            remote_database,
            remote_table,
            replace_query,
            on_duplicate_clause,
            args.columns,
            args.constraints,
            args.context);
    },
    {
        .source_access_type = AccessType::MYSQL,
    });
}

}

#endif
