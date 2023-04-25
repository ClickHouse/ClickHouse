#include <Interpreters/InterpreterShowIndexesQuery.h>

#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTShowIndexesQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <IO/Operators.h>
#include <boost/algorithm/string.hpp>


namespace DB
{


InterpreterShowIndexesQuery::InterpreterShowIndexesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_)
    , query_ptr(query_ptr_)
{
}


String InterpreterShowIndexesQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowIndexesQuery &>();

    String database;
    String table;
    if (query.from_table.contains("."))
    {
        /// FROM <db>.<table> (abbreviated form)
        chassert(query.from_database.empty());
        std::vector<String> split;
        boost::split(split, query.from_table, boost::is_any_of("."));
        chassert(split.size() == 2);
        database = split[0];
        table = split[1];
    }
    else if (query.from_database.empty())
    {
        /// FROM <table>
        chassert(!query.from_table.empty());
        database = getContext()->getCurrentDatabase();
        table = query.from_table;
    }
    else
    {
        /// FROM <database> FROM <table>
        chassert(!query.from_database.empty());
        chassert(!query.from_table.empty());
        database = query.from_database;
        table = query.from_table;
    }

    WriteBufferFromOwnString where_expression_buf;
    if (query.where_expression)
        where_expression_buf << "WHERE (" << query.where_expression << ")";
    String where_expression = where_expression_buf.str();

    WriteBufferFromOwnString rewritten_query;
    rewritten_query     << "SELECT * FROM ("
                        << "(SELECT "
                            << "name AS table, "
                            << "0 AS non_unique, "
                            << "'PRIMARY' AS key_name, "
                            << "NULL AS seq_in_index, "
                            << "NULL AS column_name, "
                            << "'A' AS collation, "
                            << "NULL AS cardinality, "
                            << "NULL AS sub_part, "
                            << "NULL AS packed, "
                            << "NULL AS null, "
                            << "'primary' AS index_type, "
                            << "NULL AS comment, "
                            << "NULL AS index_comment, "
                            << "'YES' AS visible, "
                            << "primary_key AS expression "
                        << "FROM system.tables "
                        << "WHERE "
                            << "database = '" << database << "' "
                            << "AND name = '" << table << "'"
                    << ") UNION ALL ("
                        << "SELECT "
                            << "table AS table, "
                            << "0 AS non_unique, "
                            << "name AS key_name, "
                            << "NULL AS seq_in_index, "
                            << "NULL AS column_name, "
                            << "NULL AS collation, "
                            << "NULL AS cardinality, "
                            << "NULL AS sub_part, "
                            << "NULL AS packed, "
                            << "NULL AS null, "
                            << "type AS index_type, "
                            << "NULL AS comment, "
                            << "NULL AS index_comment, "
                            << "'YES' AS visible, "
                            << "expr AS expression "
                        << "FROM system.data_skipping_indices "
                        << "WHERE "
                            << "database = '" << database << "' "
                            << "AND table = '" << table << "'"
                            << ")) "
                        << where_expression;

    /// Sorting is strictly speaking not necessary but 1. it is convenient for users, 2. SQL currently does not allow to
    /// sort the output of SHOW COLUMNS otherwise (SELECT * FROM (SHOW COLUMNS ...) ORDER BY ...) is rejected) and 3. some
    /// SQL tests can take advantage of this.
    rewritten_query << " ORDER BY index_type, expression";

    return rewritten_query.str();
}


BlockIO InterpreterShowIndexesQuery::execute()
{
    return executeQuery(getRewrittenQuery(), getContext(), true);
}


}

