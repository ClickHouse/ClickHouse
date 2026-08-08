#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Common/DateLUT.h>
#include <Common/assert_cast.h>

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

/// Render a `timestamp` type name, decoding the fractional-second precision carried in the type modifier.
/// PostgreSQL stores the precision (0..6) directly in the modifier for the time types; a negative modifier
/// (in particular the default -1) means no precision was specified, so the bare name is used. This is what
/// lets a self-connected `DateTime` / `DateTime64(p)` round-trip through schema inference in
/// `fetchPostgreSQLTableStructure` instead of collapsing to `DateTime64(6)`.
String pgFormatTimestamp(Int64 typmod, const char * suffix)
{
    if (typmod < 0)
        return fmt::format("timestamp {}", suffix);
    return fmt::format("timestamp({}) {}", typmod, suffix);
}

/// PostgreSQL type OID -> human readable name, in the spelling produced by PostgreSQL's `format_type`.
/// The set mirrors the built-in types advertised by ClickHouse's `pg_type` emulation (see PostgreSQLHandler).
/// Unknown OIDs are rendered as `text`, matching how the counterpart mapping in
/// `convertPostgreSQLDataType` treats unrecognised type names (they become `String`).
/// The type modifier is honoured for `numeric`, where it carries the precision and scale: this is what
/// lets a self-connected `Decimal(p, s)` (and wide integer types encoded as `numeric(p, 0)`) round-trip
/// through schema inference in `fetchPostgreSQLTableStructure` instead of collapsing to bare `numeric`.
/// It is likewise honoured for `timestamp`, where it carries the fractional-second precision.
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
        case 26: return "oid";
        case 700: return "real";
        case 701: return "double precision";
        case 1042: return "character";
        case 1043: return "character varying";
        case 1082: return "date";
        case 1114: return pgFormatTimestamp(typmod, "without time zone");
        case 1184: return pgFormatTimestamp(typmod, "with time zone");
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
        case 1115: return pgFormatTimestamp(typmod, "without time zone") + "[]";
        case 1182: return "date[]";
        case 1231: return pgFormatNumeric(typmod) + "[]";
        case 2951: return "uuid[]";

        default: return "text";
    }
}

/// `format_type(type_oid, typemod)` - PostgreSQL compatibility function.
/// Returns the SQL name of a type given its OID, honouring the type modifier where PostgreSQL does
/// (the `numeric` precision and scale, the `timestamp` fractional-second precision).
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

/// Render a schema name as one element of a PostgreSQL `search_path`.
///
/// `search_path` is not an arbitrary string: PostgreSQL parses it as a comma-separated list of
/// identifiers, down-casing every element that is not double-quoted and splitting on every comma that
/// is not inside quotes. A ClickHouse database name is under no such restriction, so a name that is not
/// already a valid unquoted identifier - `Mixed Case`, `a,b`, `1st`, `$user` - has to be quoted, or a
/// client that resolves unqualified names through `search_path` would look in a different schema than
/// the one `current_schema()` reports and the server itself uses. Inside the quotes a `"` is doubled,
/// as in PostgreSQL's `quote_ident`.
String quoteSearchPathElement(const String & name)
{
    /// An unquoted identifier is a lowercase letter or `_` followed by lowercase letters, digits, `_`
    /// or `$`. Anything else (including an empty name and any non-ASCII byte, whose case folding is
    /// encoding-dependent) is quoted.
    bool needs_quoting = name.empty();
    for (size_t i = 0; !needs_quoting && i < name.size(); ++i)
    {
        const char c = name[i];
        const bool lowercase_letter = c >= 'a' && c <= 'z';
        const bool digit = c >= '0' && c <= '9';
        needs_quoting = i == 0 ? !(lowercase_letter || c == '_') : !(lowercase_letter || digit || c == '_' || c == '$');
    }

    if (!needs_quoting)
        return name;

    String result;
    result.reserve(name.size() + 2);
    result += '"';
    for (const char c : name)
    {
        if (c == '"')
            result += '"';
        result += c;
    }
    result += '"';
    return result;
}

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
    static FunctionPtr create(ContextPtr context)
    {
        /// The effective timezone of the query, as the `timezone` function reports it: `DateLUT::instance`
        /// resolves the `session_timezone` setting of the current query context and falls back to the
        /// server timezone when it is not set. Capturing a constant here is what `timezone` does too.
        return std::make_shared<FunctionCurrentSetting>(context->getCurrentDatabase(), String(DateLUT::instance().getTimeZone()));
    }

    FunctionCurrentSetting(String current_database_, String current_timezone_)
        : current_database(std::move(current_database_)), current_timezone(std::move(current_timezone_))
    {
    }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }
    /// The result depends on the session: the current database (`search_path`) and the effective
    /// timezone (which follows the `session_timezone` setting).
    bool isDeterministic() const override { return false; }

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
    bool lookup(const String & setting_name, String & value) const
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
            value = current_timezone;
        else if (setting_name == "search_path")
            /// Unqualified table names resolve in the connected database (`current_schema()` is
            /// `currentDatabase`), so report exactly that - not PostgreSQL's default `public`. A client
            /// that discovers the default schema through this function must arrive at the same place the
            /// server itself resolves unqualified names in, which is why the name is rendered as a
            /// `search_path` element rather than pasted in raw.
            value = quoteSearchPathElement(current_database);
        else
            return false;
        return true;
    }

    String current_database;
    String current_timezone;
};

/// `array_position(array, element)` - PostgreSQL compatibility function.
/// Returns the 1-based position of the first occurrence of `element` in `array`, or `NULL` when the
/// element does not occur (where ClickHouse's native `indexOf`, whose search this function reuses,
/// returns 0). Provided so that the schema-discovery query of `fetchPostgreSQLTableStructure` - which
/// resolves an unqualified table name through the schemas of the `search_path` in order, the way
/// PostgreSQL itself does - can run unchanged against ClickHouse's PostgreSQL wire protocol emulation.
/// PostgreSQL's `NULL` semantics are followed: a `NULL` element is a genuine search key that finds the
/// first `NULL` of the array (`array_position([NULL, 1], NULL)` is `1`), which requires opting out of
/// the common default-null shortcut of ClickHouse functions; `indexOf` underneath treats `NULL` as a
/// normal value. A `NULL` in place of the array yields `NULL`, as in PostgreSQL.
class FunctionArrayPosition final : public IFunction
{
public:
    static constexpr auto name = "array_position";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionArrayPosition>(FunctionFactory::instance().get("indexOf", context));
    }

    explicit FunctionArrayPosition(FunctionOverloadResolverPtr index_of_) : index_of(std::move(index_of_)) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    /// The default-null wrapper would short-circuit a `NULL` element to `NULL`, but in PostgreSQL a
    /// `NULL` element searches for the first `NULL` of the array. `indexOf` handles `NULL` arguments
    /// itself; the only `NULL` this function has to produce is for a `NULL` array.
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        /// A literal `NULL` array (`Nullable` cannot wrap `Array`, so `Nullable(Nothing)` is the only
        /// nullable shape the array argument can take) is handled here; the rest of the argument
        /// validation is delegated to `indexOf`, which throws for a non-array first argument or an
        /// element type incomparable with the array elements.
        if (isNothing(removeNullable(arguments[0].type)))
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());

        return std::make_shared<DataTypeNullable>(index_of->build(arguments)->getResultType());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// A literal `NULL` array: the result is all-`NULL`.
        if (isNothing(removeNullable(arguments[0].type)))
            return result_type->createColumnConst(input_rows_count, Field())->convertToFullColumnIfConst();

        auto index_of_base = index_of->build(arguments);
        ColumnPtr positions = index_of_base->execute(arguments, index_of_base->getResultType(), input_rows_count, /* dry_run = */ false);
        positions = positions->convertToFullColumnIfConst();

        const auto & positions_data = assert_cast<const ColumnUInt64 &>(*positions).getData();
        auto null_map = ColumnUInt8::create(input_rows_count);
        auto & null_map_data = null_map->getData();
        for (size_t i = 0; i < input_rows_count; ++i)
            null_map_data[i] = positions_data[i] == 0 ? 1 : 0;

        return ColumnNullable::create(positions, std::move(null_map));
    }

private:
    FunctionOverloadResolverPtr index_of;
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

    factory.registerFunction<FunctionArrayPosition>(FunctionDocumentation{
        .description = "PostgreSQL compatibility function. Returns the 1-based position of the first occurrence of the "
                       "element in the array, or `NULL` when the element does not occur. "
                       "A `NULL` element searches for the first `NULL` of the array, as in PostgreSQL. "
                       "The native ClickHouse counterpart is `indexOf`, which returns 0 instead of `NULL`.",
        .syntax = "array_position(array, element)",
        .arguments = {{"array", "The array to search.", {"Array(T)"}}, {"element", "The element to search for.", {"Any"}}},
        .returned_value = {"The 1-based position of the first occurrence, or `NULL` when the element does not occur.", {"Nullable(UInt64)"}},
        .examples = {{"Example", "SELECT array_position(['a', 'b'], 'b')", "2"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Array});
}

}
