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
#include <Common/quoteString.h>
#include <Core/PostgreSQL/Utils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
}


template<typename T>
std::unordered_set<std::string> fetchPostgreSQLTablesList(T & tx)
{
    std::unordered_set<std::string> tables;
    std::string query = "SELECT tablename FROM pg_catalog.pg_tables "
        "WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";

    for (auto table_name : tx.template stream<std::string>(query))
        tables.insert(std::get<0>(table_name));

    return tables;
}


static DataTypePtr convertPostgreSQLDataType(String & type, const std::function<void()> & recheck_array, bool is_nullable = false, uint16_t dimensions = 0)
{
    DataTypePtr res;
    bool is_array = false;

    /// Get rid of trailing '[]' for arrays
    if (type.ends_with("[]"))
    {
        is_array = true;

        while (type.ends_with("[]"))
            type.resize(type.size() - 2);
    }

    if (type == "smallint")
        res = std::make_shared<DataTypeInt16>();
    else if (type == "integer")
        res = std::make_shared<DataTypeInt32>();
    else if (type == "bigint")
        res = std::make_shared<DataTypeInt64>();
    else if (type == "boolean")
        res = std::make_shared<DataTypeUInt8>();
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
        /// Numeric and decimal will both end up here as numeric. If it has type and precision,
        /// there will be Numeric(x, y), otherwise just Numeric
        UInt32 precision, scale;
        if (type.ends_with(")"))
        {
            res = DataTypeFactory::instance().get(type);
            precision = getDecimalPrecision(*res);
            scale = getDecimalScale(*res);

            if (precision <= DecimalUtils::max_precision<Decimal32>)
                res = std::make_shared<DataTypeDecimal<Decimal32>>(precision, scale);
            else if (precision <= DecimalUtils::max_precision<Decimal64>) //-V547
                res = std::make_shared<DataTypeDecimal<Decimal64>>(precision, scale);
            else if (precision <= DecimalUtils::max_precision<Decimal128>) //-V547
                res = std::make_shared<DataTypeDecimal<Decimal128>>(precision, scale);
            else if (precision <= DecimalUtils::max_precision<Decimal256>) //-V547
                res = std::make_shared<DataTypeDecimal<Decimal256>>(precision, scale);
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Precision {} and scale {} are too big and not supported", precision, scale);
        }
        else
        {
            precision = DecimalUtils::max_precision<Decimal128>;
            scale = precision / 2;
            res = std::make_shared<DataTypeDecimal<Decimal128>>(precision, scale);
        }
    }

    if (!res)
        res = std::make_shared<DataTypeString>();
    if (is_nullable)
        res = std::make_shared<DataTypeNullable>(res);

    if (is_array)
    {
        /// In some cases att_ndims does not return correct number of dimensions
        /// (it might return incorrect 0 number, for example, when a postgres table is created via 'as select * from table_with_arrays').
        /// So recheck all arrays separately afterwards. (Cannot check here on the same connection because another query is in execution).
        if (!dimensions)
        {
            /// Return 1d array type and recheck all arrays dims with array_ndims
            res = std::make_shared<DataTypeArray>(res);
            recheck_array();
        }
        else
        {
            while (dimensions--)
                res = std::make_shared<DataTypeArray>(res);
        }
    }

    return res;
}


template<typename T>
std::shared_ptr<NamesAndTypesList> readNamesAndTypesList(
        T & tx, const String & postgres_table_name, const String & query, bool use_nulls, bool only_names_and_types)
{
    auto columns = NamesAndTypes();

    try
    {
        std::set<size_t> recheck_arrays_indexes;
        {
            auto stream{pqxx::stream_from::query(tx, query)};

            size_t i = 0;
            auto recheck_array = [&]() { recheck_arrays_indexes.insert(i); };

            if (only_names_and_types)
            {
                std::tuple<std::string, std::string> row;
                while (stream >> row)
                {
                    columns.push_back(NameAndTypePair(std::get<0>(row), convertPostgreSQLDataType(std::get<1>(row), recheck_array)));
                    ++i;
                }
            }
            else
            {
                std::tuple<std::string, std::string, std::string, uint16_t> row;
                while (stream >> row)
                {
                    auto data_type = convertPostgreSQLDataType(std::get<1>(row),
                                                               recheck_array,
                                                               use_nulls && (std::get<2>(row) == "f"), /// 'f' means that postgres `not_null` is false, i.e. value is nullable
                                                               std::get<3>(row));
                    columns.push_back(NameAndTypePair(std::get<0>(row), data_type));
                    ++i;
                }
            }

            stream.complete();
        }

        for (const auto & i : recheck_arrays_indexes)
        {
            const auto & name_and_type = columns[i];

            /// All rows must contain the same number of dimensions, so limit 1 is ok. If number of dimensions in all rows is not the same -
            /// such arrays are not able to be used as ClickHouse Array at all.
            pqxx::result result{tx.exec(fmt::format("SELECT array_ndims({}) FROM {} LIMIT 1", name_and_type.name, postgres_table_name))};
            auto dimensions = result[0][0].as<int>();

            /// It is always 1d array if it is in recheck.
            DataTypePtr type = assert_cast<const DataTypeArray *>(name_and_type.type.get())->getNestedType();
            while (dimensions--)
                type = std::make_shared<DataTypeArray>(type);

            columns[i] = NameAndTypePair(name_and_type.name, type);
        }
    }

    catch (const pqxx::undefined_table &)
    {
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "PostgreSQL table {} does not exist", postgres_table_name);
    }
    catch (Exception & e)
    {
        e.addMessage("while fetching postgresql table structure");
        throw;
    }

    return !columns.empty() ? std::make_shared<NamesAndTypesList>(columns.begin(), columns.end()) : nullptr;
}


template<typename T>
PostgreSQLTableStructure fetchPostgreSQLTableStructure(
        T & tx, const String & postgres_table_name, bool use_nulls, bool with_primary_key, bool with_replica_identity_index)
{
    PostgreSQLTableStructure table;

    std::string query = fmt::format(
           "SELECT attname AS name, format_type(atttypid, atttypmod) AS type, "
           "attnotnull AS not_null, attndims AS dims "
           "FROM pg_attribute "
           "WHERE attrelid = {}::regclass "
           "AND NOT attisdropped AND attnum > 0", quoteString(postgres_table_name));

    table.columns = readNamesAndTypesList(tx, postgres_table_name, query, use_nulls, false);

    if (with_primary_key)
    {
        /// wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        query = fmt::format(
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type "
                "FROM pg_index i "
                "JOIN pg_attribute a ON a.attrelid = i.indrelid "
                "AND a.attnum = ANY(i.indkey) "
                "WHERE  i.indrelid = {}::regclass AND i.indisprimary", quoteString(postgres_table_name));

        table.primary_key_columns = readNamesAndTypesList(tx, postgres_table_name, query, use_nulls, true);
    }

    if (with_replica_identity_index && !table.primary_key_columns)
    {
        query = fmt::format(
            "SELECT "
            "a.attname AS column_name, " /// column name
            "format_type(a.atttypid, a.atttypmod) as type " /// column type
            "FROM "
            "pg_class t, "
            "pg_class i, "
            "pg_index ix, "
            "pg_attribute a "
            "WHERE "
            "t.oid = ix.indrelid "
            "and i.oid = ix.indexrelid "
            "and a.attrelid = t.oid "
            "and a.attnum = ANY(ix.indkey) "
            "and t.relkind = 'r' " /// simple tables
            "and t.relname = {} " /// Connection is already done to a needed database, only table name is needed.
            "and ix.indisreplident = 't' " /// index is is replica identity index
            "ORDER BY a.attname", /// column names
        quoteString(postgres_table_name));

        table.replica_identity_columns = readNamesAndTypesList(tx, postgres_table_name, query, use_nulls, true);
    }

    return table;
}


PostgreSQLTableStructure fetchPostgreSQLTableStructure(pqxx::connection & connection, const String & postgres_table_name, bool use_nulls)
{
    pqxx::ReadTransaction tx(connection);
    auto result = fetchPostgreSQLTableStructure(tx, postgres_table_name, use_nulls, false, false);
    tx.commit();
    return result;
}


std::unordered_set<std::string> fetchPostgreSQLTablesList(pqxx::connection & connection)
{
    pqxx::ReadTransaction tx(connection);
    auto result = fetchPostgreSQLTablesList(tx);
    tx.commit();
    return result;
}


template
PostgreSQLTableStructure fetchPostgreSQLTableStructure(
        pqxx::ReadTransaction & tx, const String & postgres_table_name, bool use_nulls,
        bool with_primary_key, bool with_replica_identity_index);

template
PostgreSQLTableStructure fetchPostgreSQLTableStructure(
        pqxx::ReplicationTransaction & tx, const String & postgres_table_name, bool use_nulls,
        bool with_primary_key, bool with_replica_identity_index);

template
std::unordered_set<std::string> fetchPostgreSQLTablesList(pqxx::work & tx);

template
std::unordered_set<std::string> fetchPostgreSQLTablesList(pqxx::ReadTransaction & tx);

}

#endif
