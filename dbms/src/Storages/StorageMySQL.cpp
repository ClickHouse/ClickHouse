#include <Storages/StorageMySQL.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>

#include <Dictionaries/MySQLBlockInputStream.h>

#include <Parsers/ASTSelectQuery.h>

#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CAN_NOT_FIND_FIELD;
};

StorageMySQL::StorageMySQL(
    const std::string & table_name_,
    const std::string & server_,
    int port_,
    const std::string & database_name_,
    const std::string & mysql_table_name_,
    const std::string & user_name_,
    const std::string & password_,
    const NamesAndTypesListPtr & columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    const Context & context_)
    : IStorage(materialized_columns_, alias_columns_, column_defaults_),
    table_name(table_name_), server(server_), port(port_), database_name(database_name_), mysql_table_name(mysql_table_name_), user_name(user_name_), password(password_), columns(columns_), context_global(context_),
    pool(database_name_, server_, user_name_, password_, port_)
{
    column_map.set_empty_key("");
    for (auto& it: *columns)
    {
        column_map[it.name] = it.type;
    }
}

std::string DumpQuery(const IAST * query)
{
    std::stringstream iss;
    IAST::FormatSettings s(iss, false, true);
    IAST::FormatState state;
    IAST::FormatStateStacked frame;
    query->formatImpl(s, state, frame);
    return iss.str();
}

std::string StorageMySQL::AnalyzeQuery(const SelectQueryInfo & query_info, const Context & context)
{
    BlockInputStreams res;
    StoragePtr storage(NULL);
    ExpressionAnalyzer analyzer(query_info.query, context, storage, *columns);
    NamesAndTypesList* usedColumns = analyzer.getColumns();
    std::stringstream iss;
    iss << "SELECT ";
    bool first = true;
    for (auto& column: *usedColumns)
    {
        if (!first)
            iss << ",";
        iss << column.name;
        first = false;
        ColumnWithTypeAndName col;
        col.name = column.name;
        google::dense_hash_map<std::string, DataTypePtr>::iterator it = column_map.find(column.name);
        if (it == column_map.end())
        {
            throw Exception("Can not find field " + column.name + " in table " + table_name, ErrorCodes::CAN_NOT_FIND_FIELD);
        }
        col.type = it->second;
        col.column = column.type->createColumn();
        sample_block.insert(std::move(col));
    }
    iss << " FROM " << mysql_table_name;
    ASTSelectQuery * select_query = typeid_cast<ASTSelectQuery *>(query_info.query.get());
    IAST* where = select_query->where_expression.get();
    if (where)
    {
        iss << " WHERE " << DumpQuery(where);
    }
    return iss.str();
}

BlockInputStreams StorageMySQL::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    DB::BlockInputStreams res;
    sample_block.clear();
    std::string query = AnalyzeQuery(query_info, context);
    res.push_back(std::make_shared<MySQLBlockInputStream>(pool.Get(), query, sample_block, max_block_size));
    return res;
}

}