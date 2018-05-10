#include <Storages/StorageMySQL.h>

#if USE_MYSQL

#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Dictionaries/MySQLBlockInputStream.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Settings.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DataStreams/ValuesRowOutputStream.h>
#include <Common/parseAddress.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <mysqlxx/Transaction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


StorageMySQL::StorageMySQL(
    const std::string & name,
    mysqlxx::Pool && pool,
    const std::string & remote_database_name,
    const std::string & remote_table_name,
    const ColumnsDescription & columns_)
    : IStorage{columns_}
    , name(name)
    , remote_database_name(remote_database_name)
    , remote_table_name(remote_table_name)
    , pool(std::move(pool))
{
}


BlockInputStreams StorageMySQL::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;
    String query = transformQueryForExternalDatabase(*query_info.query, getColumns().ordinary, remote_database_name, remote_table_name, context);

    Block sample_block;
    for (const String & name : column_names)
    {
        auto column_data = getColumn(name);
        sample_block.insert({ column_data.type, column_data.name });
    }

    return { std::make_shared<MySQLBlockInputStream>(pool.Get(), query, sample_block, max_block_size) };
}


class StorageMySQLBlockOutputStream : public IBlockOutputStream
{
public:
    explicit StorageMySQLBlockOutputStream(
            const StorageMySQL & storage,
            const std::string & remote_database_name,
            const std::string & remote_table_name ,
            const mysqlxx::PoolWithFailover::Entry & entry,
            const size_t & mysql_max_rows_to_insert)
        : sample_block{storage.getSampleBlock()}
        , remote_database_name{remote_database_name}
        , remote_table_name{remote_table_name}
        , entry{entry}
        , max_batch_rows{mysql_max_rows_to_insert}
    {
    }

   Block getHeader() const override { return sample_block; }

   void write(const Block & block) override
   {
       auto blocks = splitBlocks(block, max_batch_rows);
       mysqlxx::Transaction trans(entry);
       try
       {
           for(const Block & batch_data : blocks)
           {
               writeBlockData(batch_data);
           }
           trans.commit();
       }
       catch(...)
       {
           trans.rollback();
           throw;
       }
   }

   void writeBlockData(const Block & block)
   {
       WriteBufferFromOwnString sqlbuf;
       sqlbuf << "INSERT INTO  " << remote_database_name << "." << remote_table_name << " ( " << block.dumpNames() << " ) VALUES ";

       auto writer = std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<ValuesRowOutputStream>(sqlbuf), sample_block);
       writer->write(block);
       sqlbuf << ";";

       auto query = this->entry->query(sqlbuf.str());
       query.execute();
   }

   Blocks splitBlocks(const Block & block, const size_t & max_rows) const
   {
       const size_t splited_block_size = ceil(block.rows() * 1.0 / max_rows);
       Blocks splitted_blocks(splited_block_size);

       for (size_t idx = 0; idx < splited_block_size; ++idx)
           splitted_blocks[idx] = block.cloneEmpty();

       const size_t columns = block.columns();
       const size_t rows = block.rows();
       size_t offsets = 0;
       size_t limits = max_batch_rows;
       for (size_t idx = 0; idx < splited_block_size; ++idx)
       {
          // For last batch, limits should be the remain size
          if(idx == splited_block_size - 1) limits = rows - offsets;
          for(size_t col_idx = 0; col_idx < columns; ++col_idx)
          {
              splitted_blocks[idx].getByPosition(col_idx).column = block.getByPosition(col_idx).column->cut(offsets, limits);
          }
          offsets += max_batch_rows;
       }

       return splitted_blocks;
   }


private:
   Block sample_block;
   std::string remote_database_name;
   std::string remote_table_name;
   mysqlxx::PoolWithFailover::Entry entry;
   size_t max_batch_rows;
};


BlockOutputStreamPtr StorageMySQL::write(
    const ASTPtr & /*query*/, const Settings & settings)
{
    return std::make_shared<StorageMySQLBlockOutputStream>(*this, remote_database_name, remote_table_name, pool.Get(), settings.mysql_max_rows_to_insert);
}

void registerStorageMySQL(StorageFactory & factory)
{
    factory.registerStorage("MySQL", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 5)
            throw Exception(
                "Storage MySQL requires exactly 5 parameters: MySQL('host:port', database, table, 'user', 'password').",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < 5; ++i)
            engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], args.local_context);

        /// 3306 is the default MySQL port.
        auto parsed_host_port = parseAddress(static_cast<const ASTLiteral &>(*engine_args[0]).value.safeGet<String>(), 3306);

        const String & remote_database = static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>();
        const String & remote_table = static_cast<const ASTLiteral &>(*engine_args[2]).value.safeGet<String>();
        const String & username = static_cast<const ASTLiteral &>(*engine_args[3]).value.safeGet<String>();
        const String & password = static_cast<const ASTLiteral &>(*engine_args[4]).value.safeGet<String>();

        mysqlxx::Pool pool(remote_database, parsed_host_port.first, username, password, parsed_host_port.second);

        return StorageMySQL::create(
            args.table_name,
            std::move(pool),
            remote_database,
            remote_table,
            args.columns);
    });
}

}

#endif
