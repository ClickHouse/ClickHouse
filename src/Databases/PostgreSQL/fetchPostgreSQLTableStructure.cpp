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


static DataTypePtr convertPostgreSQLDataType(String & type, bool is_nullable, uint16_t dimensions, const std::function<void()> & recheck_array)
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


std::shared_ptr<NamesAndTypesList> fetchPostgreSQLTableStructure(
    postgres::ConnectionHolderPtr connection_holder, const String & postgres_table_name, bool use_nulls)
{
    auto columns = NamesAndTypes();

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
        std::set<size_t> recheck_arrays_indexes;
        {
            pqxx::read_transaction tx(connection_holder->get());
            auto stream{pqxx::stream_from::query(tx, query)};

            std::tuple<std::string, std::string, std::string, uint16_t> row;
            size_t i = 0;
            auto recheck_array = [&]() { recheck_arrays_indexes.insert(i); };
            while (stream >> row)
            {
                auto data_type = convertPostgreSQLDataType(std::get<1>(row),
                                                        use_nulls && (std::get<2>(row) == "f"), /// 'f' means that postgres `not_null` is false, i.e. value is nullable
                                                        std::get<3>(row),
                                                        recheck_array);
                columns.push_back(NameAndTypePair(std::get<0>(row), data_type));
                ++i;
            }
            stream.complete();
            tx.commit();
        }

        for (const auto & i : recheck_arrays_indexes)
        {
            const auto & name_and_type = columns[i];

            pqxx::nontransaction tx(connection_holder->get());
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
        throw Exception(fmt::format(
                    "PostgreSQL table {}.{} does not exist",
                    connection_holder->get().dbname(), postgres_table_name), ErrorCodes::UNKNOWN_TABLE);
    }
    catch (Exception & e)
    {
        e.addMessage("while fetching postgresql table structure");
        throw;
    }

    if (columns.empty())
        return nullptr;

    return std::make_shared<NamesAndTypesList>(NamesAndTypesList(columns.begin(), columns.end()));
}

}

#endif
