#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>

#include <Common/config_version.h>

#include <Poco/String.h>

#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_SETTING;
}

namespace
{

/// PostgreSQL encodes the length of a variable-length header in the low bytes of a type modifier and
/// offsets the whole modifier by `VARHDRSZ` (4). A modifier of -1 means "unspecified".
constexpr Int64 VARHDRSZ = 4;

/// Render a `numeric` type name, decoding the precision and scale carried in the type modifier. PostgreSQL
/// encodes them as `((precision << 16) | scale) + VARHDRSZ`. A modifier below `VARHDRSZ` (in particular the
/// default -1) means no precision was specified, so the bare name is used.
String pgFormatNumeric(Int64 typmod)
{
    if (typmod < VARHDRSZ)
        return "numeric";
    const Int64 tm = typmod - VARHDRSZ;
    const Int64 precision = (tm >> 16) & 0xFFFF;
    const Int64 scale = tm & 0xFFFF;
    return fmt::format("numeric({},{})", precision, scale);
}

/// PostgreSQL type OID -> human readable name, in the spelling produced by PostgreSQL's `format_type`.
/// The set mirrors the built-in types advertised by ClickHouse's `pg_type` emulation (see PostgreSQLHandler).
/// Unknown OIDs are rendered as `text`, matching how the counterpart mapping in
/// `convertPostgreSQLDataType` treats unrecognised type names (they become `String`).
/// The type modifier is honoured for `numeric`, where it carries the precision and scale: this is what
/// lets a self-connected `Decimal(p, s)` (and wide integer types encoded as `numeric(p, 0)`) round-trip
/// through schema inference in `fetchPostgreSQLTableStructure` instead of collapsing to bare `numeric`.
/// Array types are rendered as `<element>[]` (regardless of the number of dimensions, which PostgreSQL
/// carries separately in `attndims`); for a `numeric` array the modifier applies to the element type, so
/// e.g. `numeric(20, 0)[]` round-trips back to `Array(Decimal(20, 0))`.
String pgFormatType(Int64 oid, Int64 typmod)
{
    switch (oid)
    {
        case 16: return "boolean";
        case 17: return "bytea";
        case 18: return "character";
        case 19: return "name";
        case 20: return "bigint";
        case 21: return "smallint";
        case 23: return "integer";
        case 25: return "text";
        case 26: return "bigint"; /// oid
        case 700: return "real";
        case 701: return "double precision";
        case 1042: return "character";
        case 1043: return "character varying";
        case 1082: return "date";
        case 1114: return "timestamp without time zone";
        case 1184: return "timestamp with time zone";
        case 1700: return pgFormatNumeric(typmod);
        case 2950: return "uuid";

        /// Array types (element OID + "[]").
        case 1000: return "boolean[]";
        case 1005: return "smallint[]";
        case 1007: return "integer[]";
        case 1009: return "text[]";
        case 1016: return "bigint[]";
        case 1021: return "real[]";
        case 1022: return "double precision[]";
        case 1115: return "timestamp without time zone[]";
        case 1182: return "date[]";
        case 1231: return pgFormatNumeric(typmod) + "[]";
        case 2951: return "uuid[]";

        default: return "text";
    }
}

/// `format_type(type_oid, typemod)` - PostgreSQL compatibility function.
/// Returns the SQL name of a type given its OID. The type modifier is accepted for compatibility
/// but ignored: the emulated catalog always stores a modifier of -1, so there is nothing to format.
/// Provided so that ClickHouse's PostgreSQL wire protocol can answer the catalog-introspection
/// queries issued by libpq/pqxx clients - in particular by ClickHouse itself when the `postgresql`
/// table function or engine points at another ClickHouse instance.
class FunctionFormatType final : public IFunction
{
public:
    static constexpr auto name = "format_type";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFormatType>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isInteger(removeNullable(arguments[0])))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be an integer type OID, got {}",
                getName(), arguments[0]->getName());

        if (!isInteger(removeNullable(arguments[1])))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of function {} must be an integer type modifier, got {}",
                getName(), arguments[1]->getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn & oid_column = *arguments[0].column;
        const IColumn & typmod_column = *arguments[1].column;

        auto result = ColumnString::create();
        result->reserve(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const String type_name = pgFormatType(oid_column.getInt(i), typmod_column.getInt(i));
            result->insertData(type_name.data(), type_name.size());
        }

        return result;
    }
};

/// `current_setting(name [, missing_ok])` - PostgreSQL compatibility function.
/// Returns the textual value of a run-time configuration parameter (GUC). Only a small set of the
/// read-only parameters that clients probe during introspection is emulated. For an unknown
/// parameter the function throws, unless `missing_ok` is `true`, in which case it returns `NULL`
/// (matching PostgreSQL, which lets a client distinguish an absent parameter from one whose value is
/// the empty string via `IS NULL` / `COALESCE`). The return type is therefore `Nullable(String)`.
class FunctionCurrentSetting final : public IFunction
{
public:
    static constexpr auto name = "current_setting";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCurrentSetting>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 1 or 2 arguments: current_setting(name [, missing_ok])", getName());

        if (!isString(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be a constant string", getName());

        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnConst * name_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!name_column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "First argument of function {} must be a constant string", getName());

        bool missing_ok = false;
        if (arguments.size() == 2)
        {
            const ColumnConst * missing_ok_column = checkAndGetColumnConst<ColumnUInt8>(arguments[1].column.get());
            if (!missing_ok_column)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Second argument of function {} must be a constant boolean", getName());
            missing_ok = missing_ok_column->getValue<UInt8>() != 0;
        }

        const String setting_name = Poco::toLower(name_column->getValue<String>());

        auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());

        String value;
        if (!lookup(setting_name, value))
        {
            if (!missing_ok)
                throw Exception(
                    ErrorCodes::UNKNOWN_SETTING, "Unrecognized configuration parameter \"{}\"", name_column->getValue<String>());

            /// `missing_ok` is `true`: an absent parameter is reported as `NULL`, not an empty string.
            return result_type->createColumnConst(input_rows_count, Field());
        }

        return result_type->createColumnConst(input_rows_count, Field(value));
    }

private:
    static bool lookup(const String & setting_name, String & value)
    {
        if (setting_name == "server_version")
            value = VERSION_STRING;
        else if (setting_name == "server_version_num")
            value = "120000"; /// Behave like a modern (>= 12) PostgreSQL so clients take the current code path.
        else if (setting_name == "server_encoding" || setting_name == "client_encoding")
            value = "UTF8";
        else if (setting_name == "standard_conforming_strings" || setting_name == "integer_datetimes")
            value = "on";
        else if (setting_name == "datestyle")
            value = "ISO, MDY";
        else if (setting_name == "timezone")
            value = "UTC";
        else if (setting_name == "search_path")
            value = "public";
        else
            return false;
        return true;
    }
};

}

REGISTER_FUNCTION(PostgresCatalog)
{
    factory.registerFunction<FunctionFormatType>(FunctionDocumentation{
        .description = "PostgreSQL compatibility function. Returns the SQL name of a type given its OID. "
                       "For `numeric` the type modifier is decoded into the precision and scale "
                       "(`numeric(p, s)`); for every other type the modifier is ignored.",
        .syntax = "format_type(type_oid, typemod)",
        .arguments = {{"type_oid", "The type OID.", {"(U)Int*"}}, {"typemod", "The type modifier.", {"(U)Int*"}}},
        .returned_value = {"The SQL name of the type.", {"String"}},
        .examples = {{"Example", "SELECT format_type(23, -1)", "integer"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});

    factory.registerFunction<FunctionCurrentSetting>(FunctionDocumentation{
        .description = "PostgreSQL compatibility function. Returns the value of a run-time configuration parameter. "
                       "Only a small set of read-only parameters used for client introspection is emulated.",
        .syntax = "current_setting(name [, missing_ok])",
        .arguments
        = {{"name", "The configuration parameter name.", {"String"}}, {"missing_ok", "Return `NULL` instead of throwing for an unknown parameter.", {"UInt8"}}},
        .returned_value = {"The value of the configuration parameter, or `NULL` for an unknown parameter when `missing_ok` is true.", {"Nullable(String)"}},
        .examples = {{"Example", "SELECT current_setting('server_version_num')", "120000"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});
}

}
