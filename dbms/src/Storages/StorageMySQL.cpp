#include <Storages/StorageMySQL.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>

#include <Dictionaries/MySQLBlockInputStream.h>

#include <Parsers/ASTSelectQuery.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>


static bool isCompatible(const DB::IAST & where)
{
    const DB::ASTFunction * where_expression = typeid_cast<const DB::ASTFunction *>(&where);
    if (where_expression)
    {
        DB::String name = where_expression->name;
        if ((name == "and") || (name == "or"))
        {
            for (const auto & expr : where_expression->arguments->children)
            {
                if (!isCompatible(*expr.get()))
                    return false;
            }
        }
        else if (name == "equals" || (name == "notEquals") || (name == "greater") || (name == "less") || (name == "lessOrEquals")
            || (name == "greaterOrEquals"))
        {
            const auto & children = where_expression->arguments->children;
            if ((children.size() != 2) || !isCompatible(*children[0].get()) || !isCompatible(*children[1].get()))
                return false;
        }
        else if (name == "not")
        {
            const auto & children = where_expression->arguments->children;
            if ((children.size() != 1) || !isCompatible(*children[0].get()))
                return false;
        }
        else
            return false;
        return true;
    }
    const DB::ASTLiteral * literal = typeid_cast<const DB::ASTLiteral *>(&where);
    if (literal)
        return true;
    const DB::ASTIdentifier * identifier = typeid_cast<const DB::ASTIdentifier *>(&where);
    if (identifier)
        return true;
    return false;
}


namespace DB
{
/** Implements storage in the MySQL database.
  * Use ENGINE = mysql(host_port, database_name, table_name, user_name, password)
  * Read only
  */

namespace ErrorCodes
{
    extern const int CANNOT_FIND_FIELD;
};


StorageMySQL::StorageMySQL(const std::string & table_name_,
    const std::string & server_,
    int port_,
    const std::string & database_name_,
    const std::string & mysql_table_name_,
    const std::string & user_name_,
    const std::string & password_,
    const NamesAndTypesList & columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    const Context & context_)
    : IStorage(materialized_columns_, alias_columns_, column_defaults_)
    , table_name(table_name_)
    , server(server_)
    , port(port_)
    , database_name(database_name_)
    , mysql_table_name(mysql_table_name_)
    , user_name(user_name_)
    , password(password_)
    , columns(columns_)
    , context_global(context_)
    , pool(database_name_, server_, user_name_, password_, port_)
{
    column_map.set_empty_key("");
    for (const auto & it : columns)
    {
        column_map[it.name] = it.type;
    }
}

void dumpWhere(const IAST & where, std::stringstream & stream)
{
    IAST::FormatSettings s(stream, false, true);
    where.format(s);
}

/** Function puts to stream compatible expressions of where statement.
  * Compatible expressions are logical expressions on logical expressions or comparisons between fields or constants
  */

void filterWhere(const IAST & where, std::stringstream & stream)
{
    String name = where.getID();
    if (name == "Function_and")
    {
        const ASTFunction * and_expression = typeid_cast<const ASTFunction *>(&where);
        bool first = true;
        for (const auto & expr : and_expression->arguments->children)
        {
            if (isCompatible(*expr.get()))
            {
                if (!first)
                    stream << " AND ";
                first = false;
                dumpWhere(*expr.get(), stream);
            }
        }
    }
    else
    {
        if (isCompatible(where))
        {
            dumpWhere(where, stream);
        }
    }
}

/** Function transformQueryForODBC builds select query of all used columns in query_info from table set by table_name parameter with where expression from filtered query_info.
  * Also it is builds sample_block with all columns, where types are found in column_map
  */

std::string transformQueryForODBC(const SelectQueryInfo & query_info,
    const Context & context,
    std::string table_name,
    NamesAndTypesListPtr columns,
    google::dense_hash_map<std::string, DataTypePtr> & column_map,
    Block & sample_block)
{
    BlockInputStreams res;
    StoragePtr storage(nullptr);
    ExpressionAnalyzer analyzer(query_info.query, context, storage, *columns);
    NamesAndTypesList & usedColumns = analyzer.getUsedColumns();
    std::stringstream iss;
    iss << "SELECT ";
    bool first = true;
    for (const auto & column : usedColumns)
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
            throw Exception("Can not find field " + column.name + " in table " + table_name, ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
        }
        col.type = it->second;
        col.column = column.type->createColumn();
        sample_block.insert(std::move(col));
    }
    iss << " FROM " << table_name;
    const ASTSelectQuery * select_query = typeid_cast<ASTSelectQuery *>(query_info.query.get());
    const IAST * where = select_query->where_expression.get();
    if (where)
    {
        std::stringstream where_stream;
        //        fprintf(stderr, "WHERE treeId: %s\n", where->getTreeID().c_str());
        filterWhere(*where, where_stream);
        std::string filtered_where = where_stream.str();
        if (filtered_where.size())
        {
            iss << " WHERE " << filtered_where;
        }
        //        fprintf(stderr, "Filtered WHERE: %s\n", filtered_where.c_str());
    }
    return iss.str();
}

BlockInputStreams StorageMySQL::read(const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;
    BlockInputStreams res;
    sample_block.clear();
    std::string query = transformQueryForODBC(query_info, context, mysql_table_name, columns, column_map, sample_block);
    //    fprintf(stderr, "Query: %s\n", query.c_str());
    res.push_back(std::make_shared<MySQLBlockInputStream>(pool.Get(), query, sample_block, max_block_size));
    return res;
}
}
