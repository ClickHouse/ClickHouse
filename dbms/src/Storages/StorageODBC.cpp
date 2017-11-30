#include <Storages/StorageODBC.h>
#include <Storages/StorageMySQL.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>

#include <Dictionaries/ODBCBlockInputStream.h>

#include <Parsers/ASTSelectQuery.h>

#include <Common/typeid_cast.h>

namespace DB
{

/** Implements storage in the ODBC database.
  * Read only
  */

namespace ErrorCodes
{
    extern const int CANNOT_FIND_FIELD;
};

StorageODBC::StorageODBC(
    const std::string & table_name_,
    const std::string & database_name_,
    const std::string & odbc_table_name_,
    const NamesAndTypesListPtr & columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    const Context & context_)
    : IStorage(materialized_columns_, alias_columns_, column_defaults_),
    table_name(table_name_), database_name(database_name_), odbc_table_name(odbc_table_name_)
    , columns(columns_), context_global(context_),
    pool("ODBC", database_name_)
{
    column_map.set_empty_key("");
    for (auto& it: *columns)
    {
        column_map[it.name] = it.type;
    }
}

BlockInputStreams StorageODBC::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    DB::BlockInputStreams res;
    sample_block.clear();
    std::string query = AnalyzeQuery(query_info, context, odbc_table_name, columns, column_map, sample_block);
    res.push_back(std::make_shared<ODBCBlockInputStream>(pool.get(), query, sample_block, max_block_size));
    return res;
}

}