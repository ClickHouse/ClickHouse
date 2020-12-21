#include <Databases/PostgreSQL/FetchFromPostgreSQL.h>

#if USE_LIBPQXX

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
    extern const int UNKNOWN_TYPE;
}

/// These functions are also used for postgresql table function

std::shared_ptr<NamesAndTypesList> fetchTableStructure(ConnectionPtr connection, const String & postgres_table_name, bool use_nulls)
{
    auto columns = NamesAndTypesList();

    std::string query = fmt::format(
           "SELECT attname AS name, format_type(atttypid, atttypmod) AS type, "
           "attnotnull AS not_null, attndims AS dims "
           "FROM pg_attribute "
           "WHERE attrelid = '{}'::regclass "
           "AND NOT attisdropped AND attnum > 0", postgres_table_name);
    pqxx::read_transaction tx(*connection);
    pqxx::stream_from stream(tx, pqxx::from_query, std::string_view(query));
    std::tuple<std::string, std::string, std::string, uint16_t> row;

    /// No rows to be fetched
    if (!stream)
        return nullptr;

    while (stream >> row)
    {
        columns.push_back(NameAndTypePair(
                std::get<0>(row),
                getDataType(std::get<1>(row), use_nulls && (std::get<2>(row) == "f"), std::get<3>(row))));
    }
    stream.complete();
    tx.commit();

    return std::make_shared<NamesAndTypesList>(columns);
}


DataTypePtr getDataType(std::string & type, bool is_nullable, uint16_t dimensions)
{
    DataTypePtr res;

    /// Get rid of trailing '[]' for arrays
    if (dimensions)
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
        /// Numeric and decimal will both end up here as numeric
        /// Will get numeric(precision, scale) string, need to extract precision and scale
        std::vector<std::string> result;
        boost::split(result, type, [](char c){ return c == '(' || c == ',' || c == ')'; });
        for (std::string & key : result)
            boost::trim(key);

        /// If precision or scale are not specified, postgres creates a column in which numeric values of
        /// any precision and scale can be stored, so may be maxPrecision may be used instead of exception
        if (result.size() < 3)
            throw Exception("Numeric lacks precision and scale in its definition", ErrorCodes::UNKNOWN_TYPE);

        uint32_t precision = std::atoi(result[1].data());
        uint32_t scale = std::atoi(result[2].data());

        if (precision <= DecimalUtils::maxPrecision<Decimal32>())
            res = std::make_shared<DataTypeDecimal<Decimal32>>(precision, scale);
        else if (precision <= DecimalUtils::maxPrecision<Decimal64>())
            res = std::make_shared<DataTypeDecimal<Decimal64>>(precision, scale);
        else if (precision <= DecimalUtils::maxPrecision<Decimal128>())
            res = std::make_shared<DataTypeDecimal<Decimal128>>(precision, scale);
    }

    if (!res)
        res = std::make_shared<DataTypeString>();
    if (is_nullable)
        res = std::make_shared<DataTypeNullable>(res);
    while (dimensions--)
        res = std::make_shared<DataTypeArray>(res);

    return res;
}

}

#endif
