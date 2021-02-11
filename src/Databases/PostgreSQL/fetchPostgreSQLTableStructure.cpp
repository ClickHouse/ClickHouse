#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>

#if USE_LIBPQXX

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <pqxx/pqxx>

#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
}


std::unordered_set<std::string> fetchPostgreSQLTablesList(PostgreSQLConnection::ConnectionPtr connection)
{
    std::unordered_set<std::string> tables;
    std::string query = "SELECT tablename FROM pg_catalog.pg_tables "
        "WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";
    pqxx::read_transaction tx(*connection);

    for (auto table_name : tx.stream<std::string>(query))
        tables.insert(std::get<0>(table_name));

    return tables;
}


static DataTypePtr convertPostgreSQLDataType(std::string & type, bool is_nullable = false, uint16_t dimensions = 0)
{
    DataTypePtr res;

    /// Get rid of trailing '[]' for arrays
    if (dimensions)
        while (type.ends_with("[]"))
            type.resize(type.size() - 2);

    if (type == "smallint")
        res = std::make_shared<DataTypeInt16>();
    else if (type == "integer")
        res = std::make_shared<DataTypeInt32>();
    else if (type == "bigint")
        res = std::make_shared<DataTypeInt64>();
    else if (type == "real")
        res = std::make_shared<DataTypeFloat32>();
    else if (type == "double precision")
        res = std::make_shared<DataTypeFloat64>();
    else if (type == "serial")
        res = std::make_shared<DataTypeUInt32>();
    else if (type == "bigserial")
        res = std::make_shared<DataTypeUInt64>();
    else if (type.starts_with("timestamp"))
        res = std::make_shared<DataTypeDateTime>();
    else if (type == "date")
        res = std::make_shared<DataTypeDate>();
    else if (type.starts_with("numeric"))
    {
        /// Numeric and decimal will both end up here as numeric.
        res = DataTypeFactory::instance().get(type);
        uint32_t precision = getDecimalPrecision(*res);
        uint32_t scale = getDecimalScale(*res);

        if (precision <= DecimalUtils::maxPrecision<Decimal32>())
            res = std::make_shared<DataTypeDecimal<Decimal32>>(precision, scale);
        else if (precision <= DecimalUtils::maxPrecision<Decimal64>())
            res = std::make_shared<DataTypeDecimal<Decimal64>>(precision, scale);
        else if (precision <= DecimalUtils::maxPrecision<Decimal128>())
            res = std::make_shared<DataTypeDecimal<Decimal128>>(precision, scale);
        else if (precision <= DecimalUtils::maxPrecision<Decimal256>())
            res = std::make_shared<DataTypeDecimal<Decimal256>>(precision, scale);
    }

    if (!res)
        res = std::make_shared<DataTypeString>();
    if (is_nullable)
        res = std::make_shared<DataTypeNullable>(res);
    while (dimensions--)
        res = std::make_shared<DataTypeArray>(res);

    return res;
}


template<typename T>
std::shared_ptr<NamesAndTypesList> readNamesAndTypesList(
        std::shared_ptr<T> tx, const String & postgres_table_name, const String & query, bool use_nulls, bool only_names_and_types)
{
    auto columns = NamesAndTypesList();

    try
    {
        pqxx::stream_from stream(*tx, pqxx::from_query, std::string_view(query));

        if (only_names_and_types)
        {
            std::tuple<std::string, std::string> row;
            while (stream >> row)
                columns.push_back(NameAndTypePair(std::get<0>(row), convertPostgreSQLDataType(std::get<1>(row))));
        }
        else
        {
            std::tuple<std::string, std::string, std::string, uint16_t> row;
            while (stream >> row)
            {
                columns.push_back(NameAndTypePair(
                        std::get<0>(row),                           /// column name
                        convertPostgreSQLDataType(
                            std::get<1>(row),                       /// data type
                            use_nulls && (std::get<2>(row) == "f"), /// 'f' means that postgres `not_null` is false
                            std::get<3>(row))));                    /// number of dimensions if data type is array
            }
        }

        stream.complete();
    }
    catch (const pqxx::undefined_table &)
    {
        throw Exception(fmt::format(
                    "PostgreSQL table {} does not exist", postgres_table_name), ErrorCodes::UNKNOWN_TABLE);
    }
    catch (Exception & e)
    {
        e.addMessage("while fetching postgresql table structure");
        throw;
    }

    return !columns.empty() ? std::make_shared<NamesAndTypesList>(columns) : nullptr;
}


template<typename T>
PostgreSQLTableStructure fetchPostgreSQLTableStructureImpl(
        std::shared_ptr<T> tx, const String & postgres_table_name, bool use_nulls, bool with_primary_key)
{
    PostgreSQLTableStructure table;

    if (postgres_table_name.find('\'') != std::string::npos
        || postgres_table_name.find('\\') != std::string::npos)
    {
        throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "PostgreSQL table name cannot contain single quote or backslash characters, passed {}",
                postgres_table_name);
    }

    std::string query = fmt::format(
           "SELECT attname AS name, format_type(atttypid, atttypmod) AS type, "
           "attnotnull AS not_null, attndims AS dims "
           "FROM pg_attribute "
           "WHERE attrelid = '{}'::regclass "
           "AND NOT attisdropped AND attnum > 0", postgres_table_name);

    table.columns = readNamesAndTypesList(tx, postgres_table_name, query, use_nulls, false);

    if (!with_primary_key)
        return table;

    /// wiki.postgresql.org/wiki/Retrieve_primary_key_columns
    query = fmt::format(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type "
            "FROM pg_index i "
            "JOIN pg_attribute a ON a.attrelid = i.indrelid "
            "AND a.attnum = ANY(i.indkey) "
            "WHERE  i.indrelid = '{}'::regclass AND i.indisprimary", postgres_table_name);

    table.primary_key_columns = readNamesAndTypesList(tx, postgres_table_name, query, use_nulls, true);

    return table;
}


PostgreSQLTableStructure fetchPostgreSQLTableStructure(
        std::shared_ptr<pqxx::read_transaction> tx, const String & postgres_table_name, bool use_nulls, bool with_primary_key)
{
    return fetchPostgreSQLTableStructureImpl<pqxx::read_transaction>(tx, postgres_table_name, use_nulls, with_primary_key);
}


/// For the case when several operations are made on the transaction object before it can be used (like export snapshot and isolation level)
PostgreSQLTableStructure fetchPostgreSQLTableStructure(
        std::shared_ptr<pqxx::work> tx, const String & postgres_table_name, bool use_nulls, bool with_primary_key)
{
    return fetchPostgreSQLTableStructureImpl<pqxx::work>(tx, postgres_table_name, use_nulls, with_primary_key);
}


PostgreSQLTableStructure fetchPostgreSQLTableStructure(
        PostgreSQLConnection::ConnectionPtr connection, const String & postgres_table_name, bool use_nulls, bool with_primary_key)
{
    auto tx = std::make_shared<pqxx::read_transaction>(*connection);
    auto table = fetchPostgreSQLTableStructure(tx, postgres_table_name, use_nulls, with_primary_key);
    tx->commit();

    return table;
}


}

#endif
