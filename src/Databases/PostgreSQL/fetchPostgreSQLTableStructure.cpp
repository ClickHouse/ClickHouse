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


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
}


std::unordered_set<std::string> fetchPostgreSQLTablesList(ConnectionPtr connection)
{
    std::unordered_set<std::string> tables;
    std::string query = "SELECT tablename FROM pg_catalog.pg_tables "
        "WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";
    pqxx::read_transaction tx(*connection);

    for (auto table_name : tx.stream<std::string>(query))
        tables.insert(std::get<0>(table_name));

    return tables;
}


static DataTypePtr convertPostgreSQLDataType(std::string & type, bool is_nullable, uint16_t dimensions)
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


std::shared_ptr<NamesAndTypesList> fetchPostgreSQLTableStructure(
    std::shared_ptr<pqxx::connection> connection, const String & postgres_table_name, bool use_nulls)
{
    auto columns = NamesAndTypesList();

    if (postgres_table_name.find('\'') != std::string::npos
        || postgres_table_name.find('\\') != std::string::npos)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PostgreSQL table name cannot contain single quote or backslash characters, passed {}",
            postgres_table_name);
    }

    std::string query = fmt::format(
           "SELECT attname AS name, format_type(atttypid, atttypmod) AS type, "
           "attnotnull AS not_null, attndims AS dims "
           "FROM pg_attribute "
           "WHERE attrelid = '{}'::regclass "
           "AND NOT attisdropped AND attnum > 0", postgres_table_name);
    try
    {
        pqxx::read_transaction tx(*connection);
        pqxx::stream_from stream(tx, pqxx::from_query, std::string_view(query));

        std::tuple<std::string, std::string, std::string, uint16_t> row;
        while (stream >> row)
        {
            columns.push_back(NameAndTypePair(
                    std::get<0>(row),
                    convertPostgreSQLDataType(
                        std::get<1>(row),
                        use_nulls && (std::get<2>(row) == "f"), /// 'f' means that postgres `not_null` is false, i.e. value is nullable
                        std::get<3>(row))));
        }
        stream.complete();
        tx.commit();
    }
    catch (const pqxx::undefined_table &)
    {
        throw Exception(fmt::format(
                    "PostgreSQL table {}.{} does not exist",
                    connection->dbname(), postgres_table_name), ErrorCodes::UNKNOWN_TABLE);
    }
    catch (Exception & e)
    {
        e.addMessage("while fetching postgresql table structure");
        throw;
    }

    if (columns.empty())
        return nullptr;

    return std::make_shared<NamesAndTypesList>(columns);
}

}

#endif
