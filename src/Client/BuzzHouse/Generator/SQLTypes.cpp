#include <Client/BuzzHouse/Generator/SQLTypes.h>
#include <Client/BuzzHouse/Generator/StatementGenerator.h>
#include <Client/BuzzHouse/Utils/HugeInt.h>
#include <Client/BuzzHouse/Utils/UHugeInt.h>

namespace BuzzHouse
{

static inline String nextFloatingPoint(RandomGenerator & rg, const bool extremes)
{
    String ret;
    const uint32_t next_option = rg.nextLargeNumber();

    if (extremes && next_option < 25)
    {
        if (next_option < 17)
        {
            ret += next_option < 9 ? "+" : "-";
        }
        ret += "nan";
    }
    else if (extremes && next_option < 49)
    {
        if (next_option < 41)
        {
            ret += next_option < 33 ? "+" : "-";
        }
        ret += "inf";
    }
    else if (extremes && next_option < 73)
    {
        if (next_option < 65)
        {
            ret += next_option < 57 ? "+" : "-";
        }
        ret += "0.0";
    }
    else if (next_option < 373)
    {
        ret = std::to_string(rg.nextRandomInt32());
    }
    else if (next_option < 673)
    {
        ret = std::to_string(rg.nextRandomInt64());
    }
    else
    {
        std::uniform_int_distribution<uint32_t> next_dist(0, 76);
        const uint32_t left = next_dist(rg.generator);
        const uint32_t right = next_dist(rg.generator);

        ret = appendDecimal(rg, false, left, right);
    }
    return ret;
}

static String numberColumnEntry(RandomGenerator & rg, const bool negative, const bool iffunc)
{
    String buf;

    buf += negative ? "(-" : "";
    buf += "number";
    buf += negative ? ")" : "";
    if (iffunc || rg.nextSmallNumber() < 4)
    {
        /// Generate identical numbers
        buf += " % ";
        buf += std::to_string(rg.randomInt<uint32_t>(2, 31));
    }
    return buf;
}

static String numberColumn(RandomGenerator & rg, const bool can_negative, String && typeName)
{
    String buf;
    const bool iffunc = rg.nextSmallNumber() < 4;

    if (iffunc)
    {
        buf += "if(";
        buf += numberColumnEntry(rg, false, true);
        buf += ",";
    }
    buf += "CAST(";
    buf += numberColumnEntry(rg, can_negative && rg.nextBool(), false);
    buf += " AS ";
    buf += typeName;
    buf += ")";
    if (iffunc)
    {
        buf += ",CAST(";
        buf += numberColumnEntry(rg, can_negative && rg.nextBool(), false);
        buf += " AS ";
        buf += typeName;
        buf += "))";
    }
    return buf;
}

String BoolType::typeName(const bool, const bool) const
{
    return "Bool";
}

String BoolType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return "BOOL";
}

String BoolType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "BOOLEAN";
}

String BoolType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "INTEGER";
}

std::unique_ptr<SQLType> BoolType::typeDeepCopy() const
{
    return std::make_unique<BoolType>();
}

String BoolType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return rg.nextBool() ? "TRUE" : "FALSE";
}

String BoolType::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t, const uint32_t) const
{
    if (rg.nextSmallNumber() < 8)
    {
        const static DB::Strings comp = {"<", "<=", ">", ">=", "=", "=", "=", "<>", "<>"};
        String buf = "(number % ";

        buf += std::to_string(rg.randomInt<uint32_t>(1, 10));
        buf += ") ";
        buf += rg.pickRandomly(comp);
        buf += " ";
        buf += std::to_string(rg.randomInt<uint32_t>(1, 10));
        return buf;
    }
    return appendRandomRawValue(rg, gen);
}

String IntType::typeName(const bool, const bool) const
{
    return fmt::format("{}Int{}", is_unsigned ? "U" : "", size);
}

String IntType::MySQLtypeName(RandomGenerator &, const bool) const
{
    switch (size)
    {
        case 8:
            return fmt::format("TINYINT {}", is_unsigned ? " UNSIGNED" : "");
        case 16:
            return fmt::format("SMALLINT {}", is_unsigned ? " UNSIGNED" : "");
        case 32:
            return fmt::format("INT {}", is_unsigned ? " UNSIGNED" : "");
        default:
            return fmt::format("BIGINT {}", is_unsigned ? " UNSIGNED" : "");
    }
}

String IntType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    switch (size)
    {
        case 8:
        case 16:
            return "SMALLINT";
        case 32:
            return "INTEGER";
        default:
            return "BIGINT";
    }
}

String IntType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "INTEGER";
}

std::unique_ptr<SQLType> IntType::typeDeepCopy() const
{
    return std::make_unique<IntType>(size, is_unsigned);
}

String IntType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    /// ~10% chance of a special boundary value
    if (rg.nextSmallNumber() < 2)
    {
        if (is_unsigned)
        {
            switch (rg.randomInt<uint32_t>(0, 2))
            {
                case 0:
                    return "0";
                case 1:
                    return "1";
                case 2: /// Maximum value per width
                    switch (size)
                    {
                        case 8:
                            return "255";
                        case 16:
                            return "65535";
                        case 32:
                            return "4294967295";
                        case 64:
                            return "18446744073709551615";
                        case 128:
                            return "340282366920938463463374607431768211455";
                        default:
                            return "115792089237316195423570985008687907853269984665640564039457584007913129639935";
                    }
                default:
                    UNREACHABLE();
            }
        }
        else
        {
            switch (rg.randomInt<uint32_t>(0, 4))
            {
                case 0:
                    return "0";
                case 1:
                    return "1";
                case 2:
                    return "-1";
                case 3: /// Minimum (most negative) value per width
                    switch (size)
                    {
                        case 8:
                            return "-128";
                        case 16:
                            return "-32768";
                        case 32:
                            return "-2147483648";
                        case 64:
                            return "-9223372036854775808";
                        case 128:
                            return "-170141183460469231731687303715884105728";
                        default:
                            return "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
                    }
                case 4: /// Maximum value per width
                    switch (size)
                    {
                        case 8:
                            return "127";
                        case 16:
                            return "32767";
                        case 32:
                            return "2147483647";
                        case 64:
                            return "9223372036854775807";
                        case 128:
                            return "170141183460469231731687303715884105727";
                        default:
                            return "57896044618658097711785492504343953926634992332820282019728792003956564819967";
                    }
                default:
                    UNREACHABLE();
            }
        }
    }

    if (is_unsigned)
    {
        switch (size)
        {
            case 8:
                return std::to_string(rg.nextRandomUInt8());
            case 16:
                return std::to_string(rg.nextRandomUInt16());
            case 32:
                return std::to_string(rg.nextRandomUInt32());
            case 64:
                return std::to_string(rg.nextRandomUInt64());
            default: {
                const UHugeInt val(rg.nextRandomUInt64(), rg.nextRandomUInt64());
                return val.toString();
            }
        }
    }
    else
    {
        switch (size)
        {
            case 8:
                return std::to_string(rg.nextRandomInt8());
            case 16:
                return std::to_string(rg.nextRandomInt16());
            case 32:
                return std::to_string(rg.nextRandomInt32());
            case 64:
                return std::to_string(rg.nextRandomInt64());
            default: {
                const HugeInt val(rg.nextRandomInt64(), rg.nextRandomUInt64());
                return val.toString();
            }
        }
    }
}

String IntType::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t, const uint32_t) const
{
    if (size > 8 && rg.nextSmallNumber() < 8)
    {
        return numberColumn(rg, !is_unsigned, typeName(false, false));
    }
    return appendRandomRawValue(rg, gen);
}

String FloatType::typeName(const bool, const bool) const
{
    return fmt::format("{}Float{}", size == 16 ? "B" : "", size);
}

String FloatType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return (size == 32) ? "FLOAT" : "DOUBLE";
}

String FloatType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return (size == 32) ? "REAL" : "DOUBLE PRECISION";
}

String FloatType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "REAL";
}

std::unique_ptr<SQLType> FloatType::typeDeepCopy() const
{
    return std::make_unique<FloatType>(size);
}

String FloatType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return nextFloatingPoint(rg, true);
}

String FloatType::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t, const uint32_t) const
{
    if (rg.nextSmallNumber() < 8)
    {
        return numberColumn(rg, true, typeName(false, false));
    }
    return appendRandomRawValue(rg, gen);
}

String DateType::typeName(const bool, const bool) const
{
    return fmt::format("Date{}", extended ? "32" : "");
}

String DateType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return "DATE";
}

String DateType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "DATE";
}

String DateType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

std::unique_ptr<SQLType> DateType::typeDeepCopy() const
{
    return std::make_unique<DateType>(extended);
}

String DateType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    const bool allow_func = gen.getAllowNotDetermistic();
    String ret = extended ? rg.nextDate32("'", allow_func) : rg.nextDate("'", allow_func);

    ret += allow_func ? fmt::format("::{}", typeName(false, false)) : "";
    return ret;
}

String DateType::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t, const uint32_t) const
{
    if (rg.nextSmallNumber() < 8)
    {
        return numberColumn(rg, false, typeName(false, false));
    }
    return appendRandomRawValue(rg, gen);
}

String TimeType::typeName(const bool, const bool) const
{
    String ret;

    ret += "Time";
    if (extended)
    {
        ret += "64";
        if (precision.has_value())
        {
            ret += "(";
            ret += std::to_string(precision.value());
            ret += ")";
        }
    }
    return ret;
}

String TimeType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return "TIME";
}

String TimeType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "TIME";
}

String TimeType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

std::unique_ptr<SQLType> TimeType::typeDeepCopy() const
{
    return std::make_unique<TimeType>(extended, precision);
}

String TimeType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    const bool allow_func = gen.getAllowNotDetermistic();
    String ret = extended ? rg.nextTime64("'", allow_func, precision.has_value()) : rg.nextTime("'", allow_func);

    ret += allow_func ? fmt::format("::{}", typeName(false, false)) : "";
    return ret;
}

String TimeType::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t, const uint32_t) const
{
    if (rg.nextSmallNumber() < 8)
    {
        return numberColumn(rg, false, typeName(false, false));
    }
    return appendRandomRawValue(rg, gen);
}

String DateTimeType::typeName(const bool escape, const bool simplified) const
{
    String ret;

    ret += "DateTime";
    if (extended)
    {
        ret += "64";
    }
    if (precision.has_value() || (!simplified && timezone.has_value()))
    {
        ret += "(";
        if (precision.has_value())
        {
            ret += std::to_string(precision.value());
        }
        if (!simplified && timezone.has_value())
        {
            if (precision.has_value())
            {
                ret += ",";
            }
            if (escape)
            {
                ret += "\\";
            }
            ret += "'";
            ret += timezone.value();
            if (escape)
            {
                ret += "\\";
            }
            ret += "'";
        }
        ret += ")";
    }
    return ret;
}

String DateTimeType::MySQLtypeName(RandomGenerator & rg, const bool) const
{
    return rg.nextBool() ? "DATETIME" : "TIMESTAMP";
}

String DateTimeType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "TIMESTAMP";
}

String DateTimeType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

std::unique_ptr<SQLType> DateTimeType::typeDeepCopy() const
{
    return std::make_unique<DateTimeType>(extended, precision, timezone);
}

String DateTimeType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    const bool allow_func = gen.getAllowNotDetermistic();
    String ret
        = extended ? rg.nextDateTime64("'", allow_func, precision.has_value()) : rg.nextDateTime("'", allow_func, precision.has_value());

    ret += allow_func ? fmt::format("::{}", typeName(false, false)) : "";
    return ret;
}

String DateTimeType::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t, const uint32_t) const
{
    if (rg.nextSmallNumber() < 8)
    {
        return numberColumn(rg, false, typeName(false, false));
    }
    return appendRandomRawValue(rg, gen);
}

String DecimalType::typeName(const bool, const bool) const
{
    String ret;

    ret += "Decimal";
    if (short_notation.has_value())
    {
        ret += DecimalN_DecimalPrecision_Name(short_notation.value()).substr(1);
        ret += "(";
        ret += std::to_string(scale.value());
        ret += ")";
    }
    else
    {
        if (precision.has_value())
        {
            ret += "(";
            ret += std::to_string(precision.value());
            if (scale.has_value())
            {
                ret += ",";
                ret += std::to_string(scale.value());
            }
            ret += ")";
        }
    }
    return ret;
}

String DecimalType::MySQLtypeName(RandomGenerator &, const bool) const
{
    String ret;

    ret += "DECIMAL";
    if (precision.has_value())
    {
        ret += "(";
        ret += std::to_string(precision.value());
        if (scale.has_value())
        {
            ret += ",";
            ret += std::to_string(scale.value());
        }
        ret += ")";
    }
    return ret;
}

String DecimalType::PostgreSQLtypeName(RandomGenerator & rg, const bool escape) const
{
    return MySQLtypeName(rg, escape);
}

String DecimalType::SQLitetypeName(RandomGenerator & rg, const bool escape) const
{
    return MySQLtypeName(rg, escape);
}

std::unique_ptr<SQLType> DecimalType::typeDeepCopy() const
{
    return std::make_unique<DecimalType>(short_notation, precision, scale);
}

String DecimalType::appendDecimalValue(RandomGenerator & rg, const bool use_func, const DecimalType * dt)
{
    const uint32_t right = dt->scale.value_or(0);
    const uint32_t left = dt->precision.value_or(9) - right;

    return appendDecimal(rg, use_func, left, right);
}

String DecimalType::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t, const uint32_t) const
{
    if (rg.nextSmallNumber() < 8)
    {
        return numberColumn(rg, true, typeName(false, false));
    }
    return appendRandomRawValue(rg, gen);
}

String DecimalType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return appendDecimalValue(rg, true, this);
}

String StringType::typeName(const bool, const bool) const
{
    if (precision.has_value())
    {
        return fmt::format("FixedString({})", precision.value());
    }
    else
    {
        return "String";
    }
}

String StringType::MySQLtypeName(RandomGenerator & rg, const bool) const
{
    if (precision.has_value())
    {
        return fmt::format("{}{}({})", rg.nextBool() ? "VAR" : "", rg.nextBool() ? "CHAR" : "BINARY", precision.value());
    }
    else
    {
        return rg.nextBool() ? "BLOB" : "TEXT";
    }
}

String StringType::PostgreSQLtypeName(RandomGenerator & rg, const bool) const
{
    if (precision.has_value())
    {
        return fmt::format("{}CHAR({})", rg.nextBool() ? "VAR" : "", precision.value());
    }
    else
    {
        return "TEXT";
    }
}

String StringType::SQLitetypeName(RandomGenerator & rg, const bool) const
{
    return rg.nextBool() ? "BLOB" : "TEXT";
}

std::unique_ptr<SQLType> StringType::typeDeepCopy() const
{
    return std::make_unique<StringType>(precision);
}

String StringType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return rg.nextString("'", true, precision.value_or(rg.nextStrlen()));
}

String StringType::insertNumberEntry(RandomGenerator & rg, StatementGenerator &, const uint32_t max_strlen, const uint32_t) const
{
    if (rg.nextSmallNumber() < 8)
    {
        return numberColumn(rg, true, "String");
    }
    return rg.nextString("'", true, std::min(max_strlen, precision.value_or(rg.nextStrlen())));
}

String UUIDType::typeName(const bool, const bool) const
{
    return "UUID";
}

String UUIDType::MySQLtypeName(RandomGenerator & rg, const bool) const
{
    return rg.nextBool() ? "BLOB" : "TEXT";
}

String UUIDType::PostgreSQLtypeName(RandomGenerator &, const bool escape) const
{
    return typeName(escape, false);
}

String UUIDType::SQLitetypeName(RandomGenerator & rg, const bool) const
{
    return rg.nextBool() ? "BLOB" : "TEXT";
}

std::unique_ptr<SQLType> UUIDType::typeDeepCopy() const
{
    return std::make_unique<UUIDType>();
}

String UUIDType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return "'" + rg.nextUUID() + "'";
}

String UUIDType::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t, const uint32_t) const
{
    return appendRandomRawValue(rg, gen);
}

String EnumType::typeName(const bool escape, const bool simplified) const
{
    String ret;

    if (simplified)
    {
        return "String";
    }
    ret += "Enum";
    ret += std::to_string(size);
    ret += "(";
    for (size_t i = 0; i < values.size(); i++)
    {
        const EnumValue & v = values[i];

        if (i != 0)
        {
            ret += ", ";
        }
        for (const auto & c : v.val)
        {
            if (escape)
            {
                switch (c)
                {
                    case '\'':
                        ret += "\\'";
                        break;
                    case '\\':
                        ret += "\\\\";
                        break;
                    case '\b':
                        ret += "\\b";
                        break;
                    case '\f':
                        ret += "\\f";
                        break;
                    case '\r':
                        ret += "\\r";
                        break;
                    case '\n':
                        ret += "\\n";
                        break;
                    case '\t':
                        ret += "\\t";
                        break;
                    case '\0':
                        ret += "\\0";
                        break;
                    case '\a':
                        ret += "\\a";
                        break;
                    case '\v':
                        ret += "\\v";
                        break;
                    default:
                        ret += c;
                }
            }
            else
            {
                ret += c;
            }
        }
        ret += " = ";
        ret += std::to_string(v.number);
    }
    ret += ")";
    return ret;
}

String EnumType::MySQLtypeName(RandomGenerator & rg, const bool) const
{
    return rg.nextBool() ? "BLOB" : "TEXT";
}

String EnumType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String EnumType::SQLitetypeName(RandomGenerator & rg, const bool) const
{
    return rg.nextBool() ? "BLOB" : "TEXT";
}

std::unique_ptr<SQLType> EnumType::typeDeepCopy() const
{
    return std::make_unique<EnumType>(size, values);
}

String EnumType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return rg.pickRandomly(values).val;
}

String EnumType::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t, const uint32_t) const
{
    return appendRandomRawValue(rg, gen);
}

String IPv4Type::typeName(const bool, const bool) const
{
    return "IPv4";
}

String IPv4Type::MySQLtypeName(RandomGenerator & rg, const bool) const
{
    return rg.nextBool() ? "BLOB" : "TEXT";
}

String IPv4Type::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String IPv4Type::SQLitetypeName(RandomGenerator & rg, const bool) const
{
    return rg.nextBool() ? "BLOB" : "TEXT";
}

std::unique_ptr<SQLType> IPv4Type::typeDeepCopy() const
{
    return std::make_unique<IPv4Type>();
}

String IPv4Type::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return "'" + rg.nextIPv4() + "'";
}

String IPv4Type::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t, const uint32_t) const
{
    if (rg.nextSmallNumber() < 8)
    {
        return numberColumn(rg, false, typeName(false, false));
    }
    return appendRandomRawValue(rg, gen);
}

String IPv6Type::typeName(const bool, const bool) const
{
    return "IPv6";
}

String IPv6Type::MySQLtypeName(RandomGenerator & rg, const bool) const
{
    return rg.nextBool() ? "BLOB" : "TEXT";
}

String IPv6Type::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String IPv6Type::SQLitetypeName(RandomGenerator & rg, const bool) const
{
    return rg.nextBool() ? "BLOB" : "TEXT";
}

std::unique_ptr<SQLType> IPv6Type::typeDeepCopy() const
{
    return std::make_unique<IPv6Type>();
}

String IPv6Type::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return "'" + rg.nextIPv6() + "'";
}

String IPv6Type::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t, const uint32_t) const
{
    return appendRandomRawValue(rg, gen);
}

String DynamicType::typeName(const bool, const bool) const
{
    return fmt::format("Dynamic{}", ntypes.has_value() ? ("(max_types=" + std::to_string(ntypes.value()) + ")") : "");
}

String DynamicType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String DynamicType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String DynamicType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

std::unique_ptr<SQLType> DynamicType::typeDeepCopy() const
{
    return std::make_unique<DynamicType>(ntypes);
}

String DynamicType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    uint32_t col_counter = 0;
    const uint64_t type_mask_backup = gen.next_type_mask;

    gen.next_type_mask = gen.fc.type_mask & ~(allow_dynamic | allow_nested);
    auto next = std::unique_ptr<SQLType>(gen.randomNextType(rg, gen.next_type_mask, col_counter, nullptr));
    gen.next_type_mask = type_mask_backup;

    String ret = next->appendRandomRawValue(rg, gen);
    if (rg.nextMediumNumber() < 4)
    {
        ret += "::";
        ret += next->typeName(false, false);
    }
    return ret;
}

String DynamicType::insertNumberEntry(
    RandomGenerator & rg, StatementGenerator & gen, const uint32_t max_strlen, const uint32_t max_nested_rows) const
{
    uint32_t col_counter = 0;
    const uint64_t type_mask_backup = gen.next_type_mask;

    gen.next_type_mask = gen.fc.type_mask & ~(allow_dynamic | allow_nested);
    auto next = std::unique_ptr<SQLType>(gen.randomNextType(rg, gen.next_type_mask, col_counter, nullptr));
    gen.next_type_mask = type_mask_backup;
    return next->insertNumberEntry(rg, gen, max_strlen, max_nested_rows);
}

String JSONType::typeName(const bool escape, const bool simplified) const
{
    String ret;

    if (simplified)
    {
        return "String";
    }
    ret += "JSON";
    for (const auto & c : desc)
    {
        if (escape)
        {
            switch (c)
            {
                case '\'':
                    ret += "\\'";
                    break;
                case '\\':
                    ret += "\\\\";
                    break;
                case '\b':
                    ret += "\\b";
                    break;
                case '\f':
                    ret += "\\f";
                    break;
                case '\r':
                    ret += "\\r";
                    break;
                case '\n':
                    ret += "\\n";
                    break;
                case '\t':
                    ret += "\\t";
                    break;
                case '\0':
                    ret += "\\0";
                    break;
                case '\a':
                    ret += "\\a";
                    break;
                case '\v':
                    ret += "\\v";
                    break;
                default:
                    ret += c;
            }
        }
        else
        {
            ret += c;
        }
    }
    return ret;
}

String JSONType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return "JSON";
}

String JSONType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "JSON";
}

String JSONType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

std::unique_ptr<SQLType> JSONType::typeDeepCopy() const
{
    std::vector<JSubType> jsubcols;

    jsubcols.reserve(subcols.size());
    for (const auto & entry : subcols)
    {
        jsubcols.emplace_back(JSubType(entry.cname, entry.subtype->typeDeepCopy()));
    }
    return std::make_unique<JSONType>(desc, std::move(jsubcols));
}

String JSONType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    std::uniform_int_distribution<int> dopt(1, gen.fc.max_depth);
    std::uniform_int_distribution<int> wopt(1, gen.fc.max_width);

    return "'" + strBuildJSON(rg, dopt(rg.generator), wopt(rg.generator)) + "'";
}

String JSONType::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t, const uint32_t) const
{
    return appendRandomRawValue(rg, gen);
}


String Nullable::typeName(const bool escape, const bool simplified) const
{
    return fmt::format("Nullable({})", subtype->typeName(escape, simplified));
}

String Nullable::MySQLtypeName(RandomGenerator & rg, const bool escape) const
{
    return subtype->MySQLtypeName(rg, escape);
}

String Nullable::PostgreSQLtypeName(RandomGenerator & rg, const bool escape) const
{
    return subtype->PostgreSQLtypeName(rg, escape);
}

String Nullable::SQLitetypeName(RandomGenerator & rg, const bool escape) const
{
    return subtype->SQLitetypeName(rg, escape);
}

std::unique_ptr<SQLType> Nullable::typeDeepCopy() const
{
    return std::make_unique<Nullable>(subtype->typeDeepCopy());
}

String Nullable::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    return rg.nextMediumNumber() < 21 ? "NULL" : subtype->appendRandomRawValue(rg, gen);
}

String
Nullable::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t max_strlen, const uint32_t max_nested_rows) const
{
    return rg.nextMediumNumber() < 21 ? "NULL" : subtype->insertNumberEntry(rg, gen, max_strlen, max_nested_rows);
}

String LowCardinality::typeName(const bool escape, const bool simplified) const
{
    return fmt::format("LowCardinality({})", subtype->typeName(escape, simplified));
}

String LowCardinality::MySQLtypeName(RandomGenerator & rg, const bool escape) const
{
    return subtype->MySQLtypeName(rg, escape);
}

String LowCardinality::PostgreSQLtypeName(RandomGenerator & rg, const bool escape) const
{
    return subtype->PostgreSQLtypeName(rg, escape);
}

String LowCardinality::SQLitetypeName(RandomGenerator & rg, const bool escape) const
{
    return subtype->SQLitetypeName(rg, escape);
}

std::unique_ptr<SQLType> LowCardinality::typeDeepCopy() const
{
    return std::make_unique<LowCardinality>(subtype->typeDeepCopy());
}

String LowCardinality::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    return subtype->appendRandomRawValue(rg, gen);
}

String LowCardinality::insertNumberEntry(
    RandomGenerator & rg, StatementGenerator & gen, const uint32_t max_strlen, const uint32_t max_nested_rows) const
{
    return subtype->insertNumberEntry(rg, gen, max_strlen, max_nested_rows);
}

String GeoType::typeName(const bool, const bool) const
{
    return GeoTypes_Name(geotype);
}

String GeoType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String GeoType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String GeoType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

std::unique_ptr<SQLType> GeoType::typeDeepCopy() const
{
    return std::make_unique<GeoType>(geotype);
}

String GeoType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return strAppendGeoValue(rg, geotype);
}

String GeoType::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t, const uint32_t) const
{
    return appendRandomRawValue(rg, gen);
}

String ArrayType::typeName(const bool escape, const bool simplified) const
{
    return fmt::format("Array({})", subtype->typeName(escape, simplified));
}

String ArrayType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT []";
}

String ArrayType::PostgreSQLtypeName(RandomGenerator & rg, const bool escape) const
{
    SQLType * nsubtype = subtype.get();
    Nullable * nl = nullptr;
    LowCardinality * lc = nullptr;

    while (true)
    {
        if ((nl = dynamic_cast<Nullable *>(nsubtype)))
        {
            nsubtype = nl->subtype.get();
        }
        else if ((lc = dynamic_cast<LowCardinality *>(nsubtype)))
        {
            nsubtype = lc->subtype.get();
        }
        else
        {
            break;
        }
    }
    if (nsubtype)
    {
        return nsubtype->PostgreSQLtypeName(rg, escape) + "[]";
    }
    else
    {
        return "INT[]";
    }
}

String ArrayType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

std::unique_ptr<SQLType> ArrayType::typeDeepCopy() const
{
    return std::make_unique<ArrayType>(subtype->typeDeepCopy());
}

String ArrayType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen, const SQLType * tp, const uint64_t limit)
{
    /// This is a hot loop, so fmt::format may not be desirable
    String ret = "[";
    for (uint64_t i = 0; i < limit; i++)
    {
        if (i != 0)
        {
            ret += ", ";
        }
        ret += tp->appendRandomRawValue(rg, gen);
    }
    ret += "]";
    return ret;
}

String ArrayType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    std::uniform_int_distribution<uint64_t> rows_dist(gen.fc.min_nested_rows, gen.fc.max_nested_rows);

    return appendRandomRawValue(rg, gen, subtype.get(), rows_dist(rg.generator));
}

String ArrayType::insertNumberEntry(
    RandomGenerator & rg, StatementGenerator & gen, const uint32_t max_strlen, const uint32_t max_nested_rows) const
{
    String ret = "[";
    std::uniform_int_distribution<uint64_t> rows_dist(gen.fc.min_nested_rows, max_nested_rows);
    const uint32_t limit = static_cast<uint32_t>(rows_dist(rg.generator));

    for (uint64_t i = 0; i < limit; i++)
    {
        if (i != 0)
        {
            ret += ", ";
        }
        ret += subtype->insertNumberEntry(rg, gen, max_strlen, max_nested_rows);
    }
    ret += "]";
    return ret;
}

String MapType::typeName(const bool escape, const bool simplified) const
{
    return fmt::format("Map({},{})", key->typeName(escape, simplified), value->typeName(escape, simplified));
}

String MapType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String MapType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String MapType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

std::unique_ptr<SQLType> MapType::typeDeepCopy() const
{
    return std::make_unique<MapType>(key->typeDeepCopy(), value->typeDeepCopy());
}

String MapType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    String ret = "map(";
    std::uniform_int_distribution<uint64_t> rows_dist(gen.fc.min_nested_rows, gen.fc.max_nested_rows);
    const uint64_t limit = rows_dist(rg.generator);

    for (uint64_t i = 0; i < limit; i++)
    {
        if (i != 0)
        {
            ret += ", ";
        }
        ret += key->appendRandomRawValue(rg, gen);
        ret += ",";
        ret += value->appendRandomRawValue(rg, gen);
    }
    ret += ")";
    return ret;
}

String
MapType::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t max_strlen, const uint32_t max_nested_rows) const
{
    String ret = "map(";
    std::uniform_int_distribution<uint64_t> rows_dist(gen.fc.min_nested_rows, max_nested_rows);
    const uint64_t limit = rows_dist(rg.generator);

    for (uint64_t i = 0; i < limit; i++)
    {
        if (i != 0)
        {
            ret += ", ";
        }
        ret += key->insertNumberEntry(rg, gen, max_strlen, max_nested_rows);
        ret += ",";
        ret += value->insertNumberEntry(rg, gen, max_strlen, max_nested_rows);
    }
    ret += ")";
    return ret;
}


String TupleType::typeName(const bool escape, const bool simplified) const
{
    String ret;

    if (nullable)
    {
        ret += "Nullable(";
    }
    ret += "Tuple(";
    for (size_t i = 0; i < subtypes.size(); i++)
    {
        const SubType & sub = subtypes[i];

        if (i != 0)
        {
            ret += ",";
        }
        if (sub.cname.has_value())
        {
            ret += "c";
            ret += std::to_string(sub.cname.value());
            ret += " ";
        }
        ret += sub.subtype->typeName(escape, simplified);
    }
    ret += ")";
    if (nullable)
    {
        ret += ")";
    }
    return ret;
}

String TupleType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String TupleType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String TupleType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

std::unique_ptr<SQLType> TupleType::typeDeepCopy() const
{
    std::vector<SubType> nsubtypes;

    nsubtypes.reserve(subtypes.size());
    for (const auto & entry : subtypes)
    {
        nsubtypes.emplace_back(SubType(entry.cname, entry.subtype->typeDeepCopy()));
    }
    return std::make_unique<TupleType>(nullable, std::move(nsubtypes));
}

String TupleType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    if (nullable && rg.nextMediumNumber() < 21)
    {
        return "NULL";
    }
    String ret = "(";
    for (const auto & entry : subtypes)
    {
        ret += entry.subtype->appendRandomRawValue(rg, gen);
        ret += ", ";
    }
    ret += ")";
    return ret;
}

String TupleType::insertNumberEntry(
    RandomGenerator & rg, StatementGenerator & gen, const uint32_t max_strlen, const uint32_t max_nested_rows) const
{
    if (nullable && rg.nextMediumNumber() < 21)
    {
        return "NULL";
    }
    String ret = "(";
    for (const auto & entry : subtypes)
    {
        ret += entry.subtype->insertNumberEntry(rg, gen, max_strlen, max_nested_rows);
        ret += ", ";
    }
    ret += ")";
    return ret;
}


String VariantType::typeName(const bool escape, const bool simplified) const
{
    String ret;

    ret += "Variant(";
    for (size_t i = 0; i < subtypes.size(); i++)
    {
        if (i != 0)
        {
            ret += ",";
        }
        ret += subtypes[i]->typeName(escape, simplified);
    }
    ret += ")";
    return ret;
}

String VariantType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String VariantType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String VariantType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

std::unique_ptr<SQLType> VariantType::typeDeepCopy() const
{
    std::vector<std::unique_ptr<SQLType>> nsubtypes;

    nsubtypes.reserve(subtypes.size());
    for (const auto & entry : subtypes)
    {
        nsubtypes.emplace_back(entry->typeDeepCopy());
    }
    return std::make_unique<VariantType>(std::move(nsubtypes));
}

String VariantType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    return subtypes.empty() ? "NULL" : rg.pickRandomly(subtypes)->appendRandomRawValue(rg, gen);
}

String VariantType::insertNumberEntry(
    RandomGenerator & rg, StatementGenerator & gen, const uint32_t max_strlen, const uint32_t max_nested_rows) const
{
    return subtypes.empty() ? "NULL" : rg.pickRandomly(subtypes)->insertNumberEntry(rg, gen, max_strlen, max_nested_rows);
}


String QBitType::typeName(const bool escape, const bool simplified) const
{
    return fmt::format("QBit({}, {})", subtype->typeName(escape, simplified), dimension);
}

String QBitType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String QBitType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String QBitType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

std::unique_ptr<SQLType> QBitType::typeDeepCopy() const
{
    return std::make_unique<QBitType>(subtype->typeDeepCopy(), dimension);
}

String QBitType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    /// This is a hot loop, so fmt::format may not be desirable
    String ret = "[";
    for (uint64_t i = 0; i < dimension; i++)
    {
        if (i != 0)
        {
            ret += ", ";
        }
        ret += subtype->appendRandomRawValue(rg, gen);
    }
    ret += "]";
    return ret;
}

String
QBitType::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t max_strlen, const uint32_t max_nested_rows) const
{
    String ret = "[";
    for (uint64_t i = 0; i < dimension; i++)
    {
        if (i != 0)
        {
            ret += ", ";
        }
        ret += subtype->insertNumberEntry(rg, gen, max_strlen, max_nested_rows);
    }
    ret += "]";
    return ret;
}


String AggregateFunctionType::typeName(const bool escape, const bool simplified) const
{
    String buf = simple ? "Simple" : "";

    buf += "AggregateFunction(";
    buf += SQLFunc_Name(aggregate).substr(4);
    for (const auto & entry : subtypes)
    {
        buf += ",";
        buf += entry->typeName(escape, simplified);
    }
    buf += ")";
    return buf;
}

String AggregateFunctionType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String AggregateFunctionType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String AggregateFunctionType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

std::unique_ptr<SQLType> AggregateFunctionType::typeDeepCopy() const
{
    std::vector<std::unique_ptr<SQLType>> nsubtypes;

    nsubtypes.reserve(subtypes.size());
    for (const auto & entry : subtypes)
    {
        nsubtypes.emplace_back(entry->typeDeepCopy());
    }
    return std::make_unique<AggregateFunctionType>(simple, aggregate, std::move(nsubtypes));
}

String AggregateFunctionType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    String ret = SQLFunc_Name(aggregate).substr(4);

    ret += "State(";
    if (!subtypes.empty())
    {
        ret += subtypes[0]->appendRandomRawValue(rg, gen);
    }
    ret += ")";
    return ret;
}

String AggregateFunctionType::insertNumberEntry(
    RandomGenerator & rg, StatementGenerator & gen, const uint32_t max_strlen, const uint32_t max_nested_rows) const
{
    String ret = SQLFunc_Name(aggregate).substr(4);

    ret += "State(";
    if (!subtypes.empty())
    {
        ret += subtypes[0]->insertNumberEntry(rg, gen, max_strlen, max_nested_rows);
    }
    ret += ")";
    return ret;
}


String NestedType::typeName(const bool escape, const bool simplified) const
{
    String ret;

    ret += "Nested(";
    for (size_t i = 0; i < subtypes.size(); i++)
    {
        const NestedSubType & sub = subtypes[i];

        if (i != 0)
        {
            ret += ",";
        }
        ret += "c";
        ret += std::to_string(sub.cname);
        ret += " ";
        ret += sub.subtype->typeName(escape, simplified);
    }
    ret += ")";
    return ret;
}

String NestedType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String NestedType::PostgreSQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String NestedType::SQLitetypeName(RandomGenerator &, const bool) const
{
    return "TEXT";
}

String NestedType::appendRandomRawValue(RandomGenerator &, StatementGenerator &) const
{
    return "TEXT";
}

String NestedType::insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, const uint32_t, const uint32_t) const
{
    return appendRandomRawValue(rg, gen);
}

std::unique_ptr<SQLType> NestedType::typeDeepCopy() const
{
    std::vector<NestedSubType> nsubtypes;

    nsubtypes.reserve(subtypes.size());
    for (const auto & entry : subtypes)
    {
        nsubtypes.emplace_back(NestedSubType(entry.cname, entry.subtype->typeDeepCopy()));
    }
    return std::make_unique<NestedType>(std::move(nsubtypes));
}

std::tuple<std::unique_ptr<SQLType>, Integers> StatementGenerator::randomIntType(RandomGenerator & rg, const uint64_t allowed_types)
{
    chassert(this->ids.empty());

    if ((allowed_types & allow_unsigned_int))
    {
        if ((allowed_types & allow_int8))
        {
            this->ids.emplace_back(1);
        }
        if ((allowed_types & allow_int16))
        {
            this->ids.emplace_back(2);
        }
        this->ids.emplace_back(3);
        if ((allowed_types & allow_int64))
        {
            this->ids.emplace_back(4);
        }
        if ((allowed_types & allow_int128))
        {
            this->ids.emplace_back(5);
            this->ids.emplace_back(6);
        }
    }
    if ((allowed_types & allow_int8))
    {
        this->ids.emplace_back(7);
    }
    if ((allowed_types & allow_int16))
    {
        this->ids.emplace_back(8);
    }
    this->ids.emplace_back(9);
    if ((allowed_types & allow_int64))
    {
        this->ids.emplace_back(10);
    }
    if ((allowed_types & allow_int128))
    {
        this->ids.emplace_back(11);
        this->ids.emplace_back(12);
    }
    const uint32_t nopt = rg.pickRandomly(this->ids);
    this->ids.clear();
    switch (nopt)
    {
        case 1:
            return std::make_tuple(std::make_unique<IntType>(8, true), Integers::UInt8);
        case 2:
            return std::make_tuple(std::make_unique<IntType>(16, true), Integers::UInt16);
        case 3:
            return std::make_tuple(std::make_unique<IntType>(32, true), Integers::UInt32);
        case 4:
            return std::make_tuple(std::make_unique<IntType>(64, true), Integers::UInt64);
        case 5:
            return std::make_tuple(std::make_unique<IntType>(128, true), Integers::UInt128);
        case 6:
            return std::make_tuple(std::make_unique<IntType>(256, true), Integers::UInt256);
        case 7:
            return std::make_tuple(std::make_unique<IntType>(8, false), Integers::Int8);
        case 8:
            return std::make_tuple(std::make_unique<IntType>(16, false), Integers::Int16);
        case 9:
            return std::make_tuple(std::make_unique<IntType>(32, false), Integers::Int32);
        case 10:
            return std::make_tuple(std::make_unique<IntType>(64, false), Integers::Int64);
        case 11:
            return std::make_tuple(std::make_unique<IntType>(128, false), Integers::Int128);
        case 12:
            return std::make_tuple(std::make_unique<IntType>(256, false), Integers::Int256);
        default:
            UNREACHABLE();
    }
}

std::tuple<std::unique_ptr<SQLType>, FloatingPoints> StatementGenerator::randomFloatType(RandomGenerator & rg, const uint64_t allowed_types)
{
    chassert(this->ids.empty());

    if ((allowed_types & allow_bfloat16))
    {
        this->ids.emplace_back(1);
    }
    if ((allowed_types & allow_float32))
    {
        this->ids.emplace_back(2);
    }
    if ((allowed_types & allow_float64))
    {
        this->ids.emplace_back(3);
    }
    const uint32_t nopt = rg.pickRandomly(this->ids);
    this->ids.clear();
    return std::make_tuple(std::make_unique<FloatType>(1 << (3 + nopt)), static_cast<FloatingPoints>(nopt));
}

std::tuple<std::unique_ptr<SQLType>, Dates> StatementGenerator::randomDateType(RandomGenerator & rg, const uint64_t allowed_types) const
{
    const bool use32 = (allowed_types & allow_date32) && rg.nextBool();
    return std::make_tuple(std::make_unique<DateType>(use32), use32 ? Dates::Date32 : Dates::Date);
}

std::unique_ptr<SQLType> StatementGenerator::randomTimeType(RandomGenerator & rg, const uint64_t allowed_types, TimeTp * dt) const
{
    const bool use64 = (allowed_types & allow_time64) && rg.nextBool();
    std::optional<uint32_t> precision;

    if (dt)
    {
        dt->set_type(use64 ? Times::Time64 : Times::Time);
    }
    if (use64 && (!(allowed_types & set_any_datetime_precision) || rg.nextSmallNumber() < 5))
    {
        precision = std::optional<uint32_t>(!(allowed_types & set_any_datetime_precision) ? 6 : (rg.nextSmallNumber() - 1));
        if (dt)
        {
            dt->set_precision(precision.value());
        }
    }
    return std::make_unique<TimeType>(use64, precision);
}

std::unique_ptr<SQLType> StatementGenerator::randomDateTimeType(RandomGenerator & rg, const uint64_t allowed_types, DateTimeTp * dt) const
{
    bool has_precision = false;
    const bool use64 = (allowed_types & allow_datetime64) && rg.nextBool();
    std::optional<uint32_t> precision;
    std::optional<String> timezone;

    if (dt)
    {
        dt->set_type(use64 ? DateTimes::DateTime64 : DateTimes::DateTime);
    }
    if (use64 && (has_precision = (!(allowed_types & set_any_datetime_precision) || rg.nextSmallNumber() < 5)))
    {
        precision = std::optional<uint32_t>(!(allowed_types & set_any_datetime_precision) ? 6 : (rg.nextSmallNumber() - 1));
        if (dt)
        {
            dt->set_precision(precision.value());
        }
    }
    if ((!use64 || has_precision) && !fc.timezones.empty() && rg.nextSmallNumber() < 5)
    {
        timezone = std::optional<String>(rg.pickRandomly(fc.timezones));
        if (dt)
        {
            dt->set_timezone(timezone.value());
        }
    }
    return std::make_unique<DateTimeType>(use64, precision, timezone);
}

std::unique_ptr<SQLType>
StatementGenerator::randomDecimalType(RandomGenerator & rg, const uint64_t allowed_types, BottomTypeName * tp) const
{
    Decimal * dec = tp ? tp->mutable_decimal() : nullptr;
    std::optional<DecimalN_DecimalPrecision> short_notation;
    std::optional<uint32_t> precision;
    std::optional<uint32_t> scale;

    if (rg.nextBool())
    {
        std::uniform_int_distribution<uint32_t> dec_range(
            1,
            static_cast<uint32_t>(
                (allowed_types & set_no_decimal_limit) ? DecimalN::DecimalPrecision_MAX
                                                       : DecimalN_DecimalPrecision::DecimalN_DecimalPrecision_D128));
        short_notation = std::optional<DecimalN_DecimalPrecision>(static_cast<DecimalN_DecimalPrecision>(dec_range(rg.generator)));
        switch (short_notation.value())
        {
            case DecimalN_DecimalPrecision::DecimalN_DecimalPrecision_D32:
                precision = std::optional<uint32_t>(9);
                break;
            case DecimalN_DecimalPrecision::DecimalN_DecimalPrecision_D64:
                precision = std::optional<uint32_t>(18);
                break;
            case DecimalN_DecimalPrecision::DecimalN_DecimalPrecision_D128:
                precision = std::optional<uint32_t>(38);
                break;
            case DecimalN_DecimalPrecision::DecimalN_DecimalPrecision_D256:
                precision = std::optional<uint32_t>(76);
                break;
        }
        scale = std::optional<uint32_t>(rg.randomInt<uint32_t>(0, precision.value()));
        if (dec)
        {
            DecimalN * dn = dec->mutable_decimaln();

            dn->set_precision(short_notation.value());
            dn->set_scale(scale.value());
        }
    }
    else
    {
        DecimalSimple * ds = dec ? dec->mutable_decimal_simple() : nullptr;

        if (rg.nextBool())
        {
            std::uniform_int_distribution<uint32_t> p_range(1, (allowed_types & set_no_decimal_limit) ? 76 : 65);

            precision = std::optional<uint32_t>(p_range(rg.generator));
            if (dec)
            {
                ds->set_precision(precision.value());
            }
            if (rg.nextBool())
            {
                scale = std::optional<uint32_t>(rg.randomInt<uint32_t>(0, precision.value()));
                if (dec)
                {
                    ds->set_scale(scale.value());
                }
            }
        }
    }
    return std::make_unique<DecimalType>(short_notation, precision, scale);
}

std::unique_ptr<SQLType> StatementGenerator::randomAggregateType(RandomGenerator & rg, const bool simple, BottomTypeName * tp)
{
    uint32_t col_counter2 = 0;
    std::vector<std::unique_ptr<SQLType>> subtypes;
    AggregateFunction * af = tp ? tp->mutable_aggr() : nullptr;
    static const std::vector<SQLFunc> available_aggrs
        = {SQLFunc::FUNCany,
           SQLFunc::FUNCanyLast,
           SQLFunc::FUNCavg,
           SQLFunc::FUNCcount,
           SQLFunc::FUNCgroupArrayArray,
           SQLFunc::FUNCgroupBitAnd,
           SQLFunc::FUNCgroupBitOr,
           SQLFunc::FUNCgroupBitXor,
           SQLFunc::FUNCgroupUniqArrayArray,
           SQLFunc::FUNCgroupUniqArrayArrayMap,
           SQLFunc::FUNCmax,
           SQLFunc::FUNCmaxMap,
           SQLFunc::FUNCmaxMappedArrays,
           SQLFunc::FUNCmin,
           SQLFunc::FUNCminMap,
           SQLFunc::FUNCminMappedArrays,
           SQLFunc::FUNCsum,
           SQLFunc::FUNCsumMap,
           SQLFunc::FUNCsumMappedArrays,
           SQLFunc::FUNCsumWithOverflow};
    SQLFunc aggr = rg.pickRandomly(available_aggrs);

    if (aggr == SQLFunc::FUNCcount && (simple || this->depth >= this->fc.max_depth))
    {
        aggr = SQLFunc::FUNCany;
    }
    if (aggr != SQLFunc::FUNCcount)
    {
        this->depth++;
        subtypes.emplace_back(
            this->randomNextType(rg, this->next_type_mask & ~(allow_nested), col_counter2, tp ? af->add_types() : nullptr));
        this->depth--;
    }
    if (tp)
    {
        af->set_simple(simple);
        af->set_aggr(aggr);
    }
    return std::make_unique<AggregateFunctionType>(simple, aggr, std::move(subtypes));
}

std::unique_ptr<SQLType>
StatementGenerator::bottomType(RandomGenerator & rg, const uint64_t allowed_types, const bool low_card, BottomTypeName * tp)
{
    std::unique_ptr<SQLType> res;

    const bool allow_floats = (allowed_types & (allow_bfloat16 | allow_float32 | allow_float64)) != 0 && this->fc.fuzz_floating_points;
    const uint32_t int_type = 40;
    const uint32_t floating_point_type = 10 * static_cast<uint32_t>(allow_floats);
    const uint32_t date_type = 15 * static_cast<uint32_t>((allowed_types & allow_dates) != 0);
    const uint32_t datetime_type = 15 * static_cast<uint32_t>((allowed_types & allow_datetimes) != 0);
    const uint32_t string_type = 30 * static_cast<uint32_t>((allowed_types & allow_strings) != 0);
    const uint32_t decimal_type = 20 * static_cast<uint32_t>(!low_card && (allowed_types & allow_decimals) != 0);
    const uint32_t bool_type = 20 * static_cast<uint32_t>((allowed_types & allow_bool) != 0);
    const uint32_t enum_type = 15 * static_cast<uint32_t>(!low_card && (allowed_types & allow_enum) != 0);
    const uint32_t uuid_type = 10 * static_cast<uint32_t>((allowed_types & allow_uuid) != 0);
    const uint32_t ipv4_type = 5 * static_cast<uint32_t>((allowed_types & allow_ipv4) != 0);
    const uint32_t ipv6_type = 5 * static_cast<uint32_t>((allowed_types & allow_ipv6) != 0);
    const uint32_t j_type = 20 * static_cast<uint32_t>(!low_card && (allowed_types & allow_JSON) != 0);
    const uint32_t dynamic_type = 30 * static_cast<uint32_t>(!low_card && (allowed_types & allow_dynamic) != 0);
    const uint32_t time_type = 15 * static_cast<uint32_t>((allowed_types & allow_time) != 0);
    const uint32_t qbit_type = 15 * static_cast<uint32_t>(!low_card && (allowed_types & allow_qbit) != 0 && allow_floats);
    const uint32_t geo_type = 10 * static_cast<uint32_t>((allowed_types & allow_geo) != 0 && allow_floats);
    const uint32_t aggr_type = 10 * static_cast<uint32_t>(!low_card && (allowed_types & allow_aggregate) != 0);
    const uint32_t simple_aggr_type
        = 10 * static_cast<uint32_t>((allowed_types & allow_simple_aggregate) != 0 && this->depth < this->fc.max_depth);

    rg.pickWeighted(
        {{int_type,
          [&]
          {
              Integers nint;

              std::tie(res, nint) = randomIntType(rg, allowed_types);
              if (tp)
              {
                  tp->set_integers(nint);
              }
          }},
         {floating_point_type,
          [&]
          {
              FloatingPoints nflo;

              std::tie(res, nflo) = randomFloatType(rg, allowed_types);
              if (tp)
              {
                  tp->set_floats(nflo);
              }
          }},
         {date_type,
          [&]
          {
              Dates dd;

              std::tie(res, dd) = randomDateType(rg, allowed_types);
              if (tp)
              {
                  tp->set_dates(dd);
              }
          }},
         {datetime_type,
          [&]
          {
              DateTimeTp * dtp = tp ? tp->mutable_datetimes() : nullptr;

              res = randomDateTimeType(rg, low_card ? (allowed_types & ~(allow_datetime64)) : allowed_types, dtp);
          }},
         {string_type,
          [&]
          {
              std::optional<uint32_t> swidth;

              if (!(allowed_types & allow_fixed_strings) || rg.nextBool())
              {
                  if (tp)
                  {
                      tp->set_standard_string(true);
                  }
              }
              else
              {
                  std::uniform_int_distribution<uint32_t> fwidth(1, fc.max_string_length);

                  swidth = std::optional<uint32_t>(rg.nextBool() ? rg.nextMediumNumber() : fwidth(rg.generator));
                  if (tp)
                  {
                      tp->set_fixed_string(swidth.value());
                  }
              }
              res = std::make_unique<StringType>(swidth);
          }},
         {decimal_type, [&] { res = randomDecimalType(rg, allowed_types, tp); }},
         {bool_type,
          [&]
          {
              if (tp)
              {
                  tp->set_boolean(true);
              }
              res = std::make_unique<BoolType>();
          }},
         {enum_type,
          [&]
          {
              const bool bits16 = rg.nextBool();
              std::vector<EnumValue> evs;
              const uint32_t nvalues = (rg.nextLargeNumber() % static_cast<uint32_t>(enum_values.size())) + 1;
              EnumDef * edef = tp ? tp->mutable_enum_def() : nullptr;

              if (edef)
              {
                  edef->set_bits(bits16);
              }
              std::shuffle(enum_values.begin(), enum_values.end(), rg.generator);
              if (bits16)
              {
                  std::shuffle(enum16_ids.begin(), enum16_ids.end(), rg.generator);
              }
              else
              {
                  std::shuffle(enum8_ids.begin(), enum8_ids.end(), rg.generator);
              }
              for (uint32_t i = 0; i < nvalues; i++)
              {
                  const String & nval = enum_values[i];
                  const int32_t num = static_cast<const int32_t>(bits16 ? enum16_ids[i] : enum8_ids[i]);

                  if (edef)
                  {
                      EnumDefValue * edf = i == 0 ? edef->mutable_first_value() : edef->add_other_values();

                      edf->set_number(num);
                      edf->set_enumv(nval);
                  }
                  evs.emplace_back(EnumValue(nval, num));
              }
              res = std::make_unique<EnumType>(bits16 ? 16 : 8, evs);
          }},
         {uuid_type,
          [&]
          {
              if (tp)
              {
                  tp->set_uuid(true);
              }
              res = std::make_unique<UUIDType>();
          }},
         {ipv4_type,
          [&]
          {
              if (tp)
              {
                  tp->set_ipv4(true);
              }
              res = std::make_unique<IPv4Type>();
          }},
         {ipv6_type,
          [&]
          {
              if (tp)
              {
                  tp->set_ipv6(true);
              }
              res = std::make_unique<IPv6Type>();
          }},
         {j_type,
          [&]
          {
              String desc;
              std::vector<JSubType> subcols;
              JSONDef * jdef = tp ? tp->mutable_jdef() : nullptr;
              const uint32_t nclauses = rg.randomInt<uint32_t>(0, 6);

              if (nclauses)
              {
                  desc += "(";
              }
              this->depth++;
              for (uint32_t i = 0; i < nclauses; i++)
              {
                  const uint32_t noption = rg.nextSmallNumber();
                  JSONDefItem * jdi = tp ? jdef->add_spec() : nullptr;

                  if (i != 0)
                  {
                      desc += ", ";
                  }
                  if (noption < 4)
                  {
                      const uint32_t max_dpaths = rg.nextBool() ? (rg.nextMediumNumber() % 5) : (rg.nextLargeNumber() % 1025);

                      if (tp)
                      {
                          jdi->set_max_dynamic_paths(max_dpaths);
                      }
                      desc += "max_dynamic_paths=";
                      desc += std::to_string(max_dpaths);
                  }
                  else if (this->depth >= this->fc.max_depth || noption < 8)
                  {
                      const uint32_t max_dtypes = rg.nextBool() ? (rg.nextMediumNumber() % 5) : (rg.nextLargeNumber() % 33);

                      if (tp)
                      {
                          jdi->set_max_dynamic_types(max_dtypes);
                      }
                      desc += "max_dynamic_types=";
                      desc += std::to_string(max_dtypes);
                  }
                  else
                  {
                      uint32_t col_counter2 = 0;
                      const uint32_t ncols = rg.randomInt<uint32_t>(1, 4);
                      JSONPathType * jpt = tp ? jdi->mutable_path_type() : nullptr;
                      ColumnPath * cp = tp ? jpt->mutable_col() : nullptr;
                      String npath;

                      for (uint32_t j = 0; j < ncols; j++)
                      {
                          String nbuf;
                          Column * col = tp ? (j == 0 ? cp->mutable_col() : cp->add_sub_cols()) : nullptr;

                          if (j != 0)
                          {
                              desc += ".";
                              npath += ".";
                          }
                          desc += '`';
                          nbuf += rg.nextJSONCol();
                          npath += nbuf;
                          desc += nbuf;
                          desc += '`';
                          if (tp)
                          {
                              col->set_column(std::move(nbuf));
                          }
                      }
                      desc += " ";

                      const uint64_t type_mask_backup = this->next_type_mask;
                      this->next_type_mask = fc.type_mask & ~(allow_nested | allow_enum);
                      auto jtp = randomNextType(rg, this->next_type_mask, col_counter2, tp ? jpt->mutable_type() : nullptr);
                      this->next_type_mask = type_mask_backup;

                      desc += jtp->typeName(false, false);
                      subcols.emplace_back(JSubType(npath, std::move(jtp)));
                  }
              }
              this->depth--;
              if (nclauses)
              {
                  desc += ")";
              }
              res = std::make_unique<JSONType>(desc, std::move(subcols));
          }},
         {dynamic_type,
          [&]
          {
              Dynamic * dyn = tp ? tp->mutable_dynamic() : nullptr;
              std::optional<uint32_t> ntypes;

              if (rg.nextBool())
              {
                  ntypes = std::optional<uint32_t>(rg.nextBool() ? rg.nextSmallNumber() : rg.randomInt<uint32_t>(1, 100));
                  if (dyn)
                  {
                      dyn->set_ntypes(ntypes.value());
                  }
              }
              res = std::make_unique<DynamicType>(ntypes);
          }},
         {time_type,
          [&]
          {
              TimeTp * tt = tp ? tp->mutable_times() : nullptr;

              res = randomTimeType(rg, low_card ? (allowed_types & ~(allow_time64)) : allowed_types, tt);
          }},
         {qbit_type,
          [&]
          {
              std::unique_ptr<SQLType> sub;
              FloatingPoints nflo;
              const uint32_t dimension = rg.nextSmallNumber();

              std::tie(sub, nflo) = randomFloatType(rg, allowed_types);
              if (tp)
              {
                  QBit * qbit = tp->mutable_qbit();

                  qbit->set_subtype(nflo);
                  qbit->set_dimension(dimension);
              }
              res = std::make_unique<QBitType>(std::move(sub), dimension);
          }},
         {geo_type,
          [&]
          {
              std::uniform_int_distribution<uint32_t> geo_range(1, static_cast<uint32_t>(GeoTypes_MAX));
              const GeoTypes gt = static_cast<GeoTypes>(geo_range(rg.generator));

              if (tp)
              {
                  tp->set_geo(gt);
              }
              res = std::make_unique<GeoType>(gt);
          }},
         {aggr_type, [&] { res = randomAggregateType(rg, false, tp); }},
         {simple_aggr_type, [&] { res = randomAggregateType(rg, true, tp); }}});

    return res;
}

std::unique_ptr<SQLType>
StatementGenerator::randomNextType(RandomGenerator & rg, const uint64_t allowed_types, uint32_t & col_counter, TopTypeName * tp)
{
    const uint32_t non_nullable_type = 70;
    const uint32_t nullable_type = 35 * static_cast<uint32_t>((allowed_types & allow_nullable) != 0);
    const uint32_t array_type = 10 * static_cast<uint32_t>((allowed_types & allow_array) != 0 && this->depth < this->fc.max_depth);
    const uint32_t map_type = 10
        * static_cast<uint32_t>((allowed_types & allow_map) != 0 && this->depth < this->fc.max_depth && this->width < this->fc.max_width);
    const uint32_t tuple_type = 10 * static_cast<uint32_t>((allowed_types & allow_tuple) != 0 && this->depth < this->fc.max_depth);
    const uint32_t variant_type = 10 * static_cast<uint32_t>((allowed_types & allow_variant) != 0 && this->depth < this->fc.max_depth);
    const uint32_t nested_type = 10
        * static_cast<uint32_t>((allowed_types & allow_nested) != 0 && this->depth < this->fc.max_depth
                                && this->width < this->fc.max_width);
    std::unique_ptr<SQLType> result;

    rg.pickWeighted(
        {{non_nullable_type,
          [&]
          {
              /// Non nullable
              const bool lcard = (allowed_types & allow_low_cardinality) != 0 && rg.nextMediumNumber() < 18;
              auto res = bottomType(
                  rg, allowed_types, lcard, tp ? (lcard ? tp->mutable_non_nullable_lcard() : tp->mutable_non_nullable()) : nullptr);
              result = lcard ? std::make_unique<LowCardinality>(std::move(res)) : std::move(res);
          }},
         {nullable_type,
          [&]
          {
              /// Nullable
              const bool lcard = (allowed_types & allow_low_cardinality) != 0 && rg.nextMediumNumber() < 18;
              std::unique_ptr<SQLType> res = std::make_unique<Nullable>(bottomType(
                  rg,
                  allowed_types & ~(allow_dynamic | allow_aggregate),
                  lcard,
                  tp ? (lcard ? tp->mutable_nullable_lcard() : tp->mutable_nullable()) : nullptr));
              result = lcard ? std::make_unique<LowCardinality>(std::move(res)) : std::move(res);
          }},
         {array_type,
          [&]
          {
              /// Array
              TopTypeName * arr = tp ? tp->mutable_array() : nullptr;

              this->depth++;
              auto k = this->randomNextType(rg, this->next_type_mask & ~(allow_nested), col_counter, arr);
              this->depth--;
              result = std::make_unique<ArrayType>(std::move(k));
          }},
         {map_type,
          [&]
          {
              /// Map
              MapTypeDef * mt = tp ? tp->mutable_map() : nullptr;

              this->depth++;
              auto k = this->randomNextType(
                  rg, this->next_type_mask & ~(allow_nullable | allow_nested), col_counter, mt ? mt->mutable_key() : nullptr);
              this->width++;
              auto v = this->randomNextType(rg, this->next_type_mask & ~(allow_nested), col_counter, mt ? mt->mutable_value() : nullptr);
              this->depth--;
              this->width--;
              result = std::make_unique<MapType>(std::move(k), std::move(v));
          }},
         {tuple_type,
          [&]
          {
              /// Tuple
              std::vector<SubType> subtypes;
              const bool with_names = rg.nextBool();
              TupleTypeDef * tt = tp ? tp->mutable_tuple() : nullptr;
              TupleWithColumnNames * twcn = (tp && with_names) ? tt->mutable_with_names() : nullptr;
              TupleWithOutColumnNames * twocn = (tp && !with_names) ? tt->mutable_no_names() : nullptr;
              const uint32_t ncols = this->width >= this->fc.max_width
                  ? 0
                  : (rg.nextMediumNumber() % std::min<uint32_t>(5, this->fc.max_width - this->width));
              const bool is_nullable = rg.nextSmallNumber() < 4;

              if (tt)
              {
                  tt->set_is_nullable(is_nullable);
              }
              this->depth++;
              for (uint32_t i = 0; i < ncols; i++)
              {
                  std::optional<uint32_t> opt_cname;
                  TypeColumnDef * tcd = twcn ? twcn->add_values() : nullptr;
                  TopTypeName * ttn = twocn ? twocn->add_values() : nullptr;

                  if (tcd)
                  {
                      const uint32_t ncname = col_counter++;

                      tcd->mutable_col()->set_column("c" + std::to_string(ncname));
                      opt_cname = std::optional<uint32_t>(ncname);
                  }
                  auto k
                      = this->randomNextType(rg, this->next_type_mask & ~(allow_nested), col_counter, tcd ? tcd->mutable_type_name() : ttn);
                  subtypes.emplace_back(SubType(opt_cname, std::move(k)));
              }
              this->depth--;
              result = std::make_unique<TupleType>(is_nullable, std::move(subtypes));
          }},
         {variant_type,
          [&]
          {
              /// Variant
              std::vector<std::unique_ptr<SQLType>> subtypes;
              TupleWithOutColumnNames * twocn = tp ? tp->mutable_variant() : nullptr;
              const uint32_t ncols
                  = (this->width >= this->fc.max_width ? 0
                                                       : (rg.nextMediumNumber() % std::min<uint32_t>(5, this->fc.max_width - this->width)))
                  + UINT32_C(1);

              this->depth++;
              for (uint32_t i = 0; i < ncols; i++)
              {
                  TopTypeName * ttn = tp ? twocn->add_values() : nullptr;

                  subtypes.emplace_back(this->randomNextType(
                      rg, this->next_type_mask & ~(allow_nullable | allow_nested | allow_variant | allow_dynamic), col_counter, ttn));
              }
              this->depth--;
              result = std::make_unique<VariantType>(std::move(subtypes));
          }},
         {nested_type,
          [&]
          {
              /// Nested
              std::vector<NestedSubType> subtypes;
              NestedTypeDef * nt = tp ? tp->mutable_nested() : nullptr;
              const uint32_t ncols = (rg.nextMediumNumber() % (std::min<uint32_t>(5, this->fc.max_width - this->width))) + UINT32_C(1);

              this->depth++;
              for (uint32_t i = 0; i < ncols; i++)
              {
                  const uint32_t cname = col_counter++;
                  TypeColumnDef * tcd = tp ? ((i == 0) ? nt->mutable_type1() : nt->add_others()) : nullptr;

                  if (tcd)
                  {
                      tcd->mutable_col()->set_column("c" + std::to_string(cname));
                  }
                  auto k = this->randomNextType(
                      rg, this->next_type_mask & ~(allow_nested), col_counter, tcd ? tcd->mutable_type_name() : nullptr);
                  subtypes.emplace_back(NestedSubType(cname, std::move(k)));
              }
              this->depth--;
              result = std::make_unique<NestedType>(std::move(subtypes));
          }}});
    return result;
}

String appendDecimal(RandomGenerator & rg, const bool use_func, const uint32_t left, const uint32_t right)
{
    String ret;

    if (use_func)
    {
        const uint32_t precision = left + right;

        ret += "toDecimal";
        if (precision <= 9)
        {
            ret += "32";
        }
        else if (precision <= 18)
        {
            ret += "64";
        }
        else if (precision <= 38)
        {
            ret += "128";
        }
        else if (precision <= 76)
        {
            ret += "256";
        }
        else
        {
            UNREACHABLE();
        }
        ret += "('";
    }

    /// ~20% chance of a special boundary value
    if (rg.nextSmallNumber() < 3)
    {
        switch (rg.randomInt<uint32_t>(0, 3))
        {
            case 0:
                /// Zero
                ret += "0.0";
                break;
            case 1:
                /// One / negative one
                ret += rg.nextBool() ? "-1.0" : "1.0";
                break;
            case 2:
                /// Maximum representable value: all nines
                ret += rg.nextBool() ? "-" : "";
                for (uint32_t j = 0; j < std::max(1u, left); j++)
                    ret += '9';
                ret += '.';
                for (uint32_t j = 0; j < std::max(1u, right); j++)
                    ret += '9';
                break;
            case 3:
                /// Smallest non-zero: 0.00...01 (or 1.0 when scale == 0)
                ret += rg.nextBool() ? "-" : "";
                ret += "0.";
                for (uint32_t j = 0; j + 1 < std::max(1u, right); j++)
                    ret += '0';
                ret += '1';
                break;
            default:
                UNREACHABLE();
        }
    }
    else
    {
        ret += rg.nextBool() ? "-" : "";
        if (left > 0)
        {
            std::uniform_int_distribution<uint32_t> next_dist(1, left);
            const uint32_t nlen = next_dist(rg.generator);

            ret += std::max<char>(rg.nextDigit(), '1');
            for (uint32_t j = 1; j < nlen; j++)
            {
                ret += rg.nextDigit();
            }
        }
        else
        {
            ret += "0";
        }
        ret += ".";
        if (right > 0)
        {
            std::uniform_int_distribution<uint32_t> next_dist(1, right);
            const uint32_t nlen = next_dist(rg.generator);

            for (uint32_t j = 0; j < nlen; j++)
            {
                ret += rg.nextDigit();
            }
        }
        else
        {
            ret += "0";
        }
    }

    if (use_func)
    {
        ret += fmt::format("', {})", right);
    }
    return ret;
}

/// Returns a single geo point as (lon, lat).
/// 80% of the time uses WGS84-bounded coordinates so geo functions see realistic input;
/// 20% of the time falls back to arbitrary values to probe edge cases.
static String nextGeoPoint(RandomGenerator & rg)
{
    if (rg.nextSmallNumber() < 9)
    {
        std::uniform_real_distribution<double> lon(-180.0, 180.0);
        std::uniform_real_distribution<double> lat(-90.0, 90.0);
        return fmt::format("({},{})", lon(rg.generator), lat(rg.generator));
    }
    return fmt::format("({},{})", nextFloatingPoint(rg, false), nextFloatingPoint(rg, false));
}

/// Returns a closed Ring: npoints unique points followed by the first point repeated.
/// An empty ring (npoints == 0) is returned as [] for degenerate-case coverage.
static String nextGeoRing(RandomGenerator & rg, const uint32_t npoints)
{
    String first_point;
    String ret = "[";
    for (uint32_t i = 0; i < npoints; i++)
    {
        if (i != 0)
            ret += ", ";
        const String pt = nextGeoPoint(rg);
        if (i == 0)
            first_point = pt;
        ret += pt;
    }
    if (npoints > 0)
        ret += ", " + first_point; /// Close the ring
    ret += "]";
    return ret;
}

String strAppendGeoValue(RandomGenerator & rg, const GeoTypes & gt)
{
    String ret;
    const uint32_t limit = rg.randomInt<uint32_t>(0, 10);
    const GeoTypes imp
        = gt == GeoTypes::Geometry ? static_cast<GeoTypes>(rg.randomInt<uint32_t>(1, static_cast<uint32_t>(GeoTypes::MultiPolygon))) : gt;

    switch (imp)
    {
        case GeoTypes::Point:
            ret = nextGeoPoint(rg);
            break;
        case GeoTypes::Ring:
            /// Closed ring: array of points where first == last
            ret = nextGeoRing(rg, limit);
            break;
        case GeoTypes::LineString:
            /// Open sequence of points, no closure requirement
            ret += "[";
            for (uint32_t i = 0; i < limit; i++)
            {
                if (i != 0)
                    ret += ", ";
                ret += nextGeoPoint(rg);
            }
            ret += "]";
            break;
        case GeoTypes::MultiLineString:
            /// Array of LineStrings (open, no closure)
            ret += "[";
            for (uint32_t i = 0; i < limit; i++)
            {
                const uint32_t npoints = rg.randomInt<uint32_t>(0, 10);
                if (i != 0)
                    ret += ", ";
                ret += "[";
                for (uint32_t j = 0; j < npoints; j++)
                {
                    if (j != 0)
                        ret += ", ";
                    ret += nextGeoPoint(rg);
                }
                ret += "]";
            }
            ret += "]";
            break;
        case GeoTypes::Polygon:
            /// Array of closed Rings: first is outer boundary, rest are holes
            ret += "[";
            for (uint32_t i = 0; i < limit; i++)
            {
                const uint32_t npoints = rg.randomInt<uint32_t>(0, 10);
                if (i != 0)
                    ret += ", ";
                ret += nextGeoRing(rg, npoints);
            }
            ret += "]";
            break;
        case GeoTypes::MultiPolygon:
            /// Array of Polygons, each an array of closed Rings
            ret += "[";
            for (uint32_t i = 0; i < limit; i++)
            {
                const uint32_t nrings = rg.randomInt<uint32_t>(0, 10);
                if (i != 0)
                    ret += ", ";
                ret += "[";
                for (uint32_t j = 0; j < nrings; j++)
                {
                    const uint32_t npoints = rg.randomInt<uint32_t>(0, 10);
                    if (j != 0)
                        ret += ", ";
                    ret += nextGeoRing(rg, npoints);
                }
                ret += "]";
            }
            ret += "]";
            break;
        case GeoTypes::Geometry:
            chassert(0);
    }
    return ret;
}

static String homogeneousJSONArray(RandomGenerator & rg)
{
    /// Homogeneous typed array: pick element type once, generate 0-5 elements of that type
    String ret;
    std::uniform_int_distribution<int> alen(0, 30);
    std::uniform_int_distribution<int> atype(1, 9);
    const int nelems = alen(rg.generator);
    const int elem_type = atype(rg.generator);

    for (int j = 0; j < nelems; j++)
    {
        if (j != 0)
            ret += ",";
        switch (elem_type)
        {
            case 1: {
                std::uniform_int_distribution<int> numbers(-1000, 1000);
                ret += std::to_string(numbers(rg.generator));
                break;
            }
            case 2:
                ret += std::to_string(rg.nextRandomInt64());
                break;
            case 3:
                ret += std::to_string(rg.nextRandomUInt64());
                break;
            case 4:
                ret += nextFloatingPoint(rg, true);
                break;
            case 5:
                ret += rg.nextString("\"", false, rg.nextStrlen());
                break;
            case 6:
                ret += rg.nextBool() ? "true" : "false";
                break;
            case 7:
                ret += "null";
                break;
            case 8:
                /// Empty string
                ret += "\"\"";
                break;
            case 9: {
                /// Decimal
                std::uniform_int_distribution<uint32_t> next_dist(0, 76);
                const uint32_t left = next_dist(rg.generator);
                const uint32_t right = next_dist(rg.generator);

                ret += appendDecimal(rg, false, left, right);
            }
            break;
            default:
                UNREACHABLE();
        }
    }
    return ret;
}

String strBuildJSONArray(RandomGenerator & rg, const int jdepth, const int jwidth)
{
    std::uniform_int_distribution<int> jopt(1, 4);
    int nelems = 0;
    int next_width = 0;

    if (jwidth)
    {
        std::uniform_int_distribution<int> alen(0, jwidth);
        nelems = alen(rg.generator);
    }
    String ret = "[";
    next_width = nelems;
    for (int j = 0; j < nelems; j++)
    {
        if (j != 0)
        {
            ret += ",";
        }
        if (jdepth)
        {
            switch (jopt(rg.generator))
            {
                case 1:
                    /// Object
                    ret += strBuildJSON(rg, jdepth - 1, next_width);
                    break;
                case 2:
                    /// Array
                    ret += strBuildJSONArray(rg, jdepth - 1, next_width);
                    break;
                case 3:
                    /// Others
                    ret += strBuildJSONElement(rg);
                    break;
                case 4:
                    /// Homogeneous array
                    ret += homogeneousJSONArray(rg);
                    break;
                default:
                    UNREACHABLE();
            }
        }
        else
        {
            ret += strBuildJSONElement(rg);
        }
        next_width--;
    }
    ret += "]";
    return ret;
}

String strBuildJSONElement(RandomGenerator & rg)
{
    String ret;
    std::uniform_int_distribution<int> opts(1, 25);

    switch (opts(rg.generator))
    {
        case 1:
            ret = "false";
            break;
        case 2:
            ret = "true";
            break;
        case 3:
            ret = "null";
            break;
        case 4:
            /// Large number
            ret = std::to_string(rg.nextRandomInt64());
            break;
        case 5:
            /// Large unsigned number
            ret = std::to_string(rg.nextRandomUInt64());
            break;
        case 6:
        case 7:
        case 8: {
            /// Small number
            std::uniform_int_distribution<int> numbers(-1000, 1000);
            ret = std::to_string(numbers(rg.generator));
        }
        break;
        case 9:
        case 10: {
            /// Decimal
            std::uniform_int_distribution<uint32_t> next_dist(0, 76);
            const uint32_t left = next_dist(rg.generator);
            const uint32_t right = next_dist(rg.generator);

            ret = appendDecimal(rg, false, left, right);
        }
        break;
        case 11:
        case 12:
        case 13:
            /// String
            ret = rg.nextString("\"", false, rg.nextStrlen());
            break;
        case 14:
            /// Date
            ret = rg.nextDate("\"", false);
            break;
        case 15:
            /// Date32
            ret = rg.nextDate32("\"", false);
            break;
        case 16:
            /// Time
            ret = rg.nextTime("\"", false);
            break;
        case 17:
            /// Time64
            ret = rg.nextTime64("\"", false, rg.nextSmallNumber() < 8);
            break;
        case 18:
            /// Datetime
            ret = rg.nextDateTime("\"", false, rg.nextSmallNumber() < 8);
            break;
        case 19:
            /// Datetime64
            ret = rg.nextDateTime64("\"", false, rg.nextSmallNumber() < 8);
            break;
        case 20:
            /// UUID
            ret = '"' + rg.nextUUID() + '"';
            break;
        case 21:
            /// IPv4
            ret = '"' + rg.nextIPv4() + '"';
            break;
        case 22:
            /// IPv6
            ret = '"' + rg.nextIPv6() + '"';
            break;
        case 23:
            /// Floating-point
            ret = nextFloatingPoint(rg, true);
            break;
        case 24:
            /// Empty string
            ret = "\"\"";
            break;
        case 25:
            /// String with escape sequences
            ret = '[' + homogeneousJSONArray(rg) + ']';
            break;
        default:
            UNREACHABLE();
    }
    return ret;
}

String strBuildJSON(RandomGenerator & rg, const int jdepth, const int jwidth)
{
    String ret = "{";

    if (jdepth && jwidth && rg.nextSmallNumber() < 9)
    {
        std::uniform_int_distribution<int> childd(0, jwidth);
        const int nchildren = childd(rg.generator);

        for (int i = 0; i < nchildren; i++)
        {
            std::uniform_int_distribution<int> jopt(1, 3);

            if (i != 0)
            {
                ret += ",";
            }
            ret += "\"";
            ret += rg.nextJSONCol();
            ret += "\":";
            switch (jopt(rg.generator))
            {
                case 1:
                    /// Object
                    ret += strBuildJSON(rg, jdepth - 1, jwidth);
                    break;
                case 2:
                    /// Array
                    ret += strBuildJSONArray(rg, jdepth - 1, jwidth);
                    break;
                case 3:
                    /// Others
                    ret += strBuildJSONElement(rg);
                    break;
                default:
                    UNREACHABLE();
            }
        }
    }
    ret += "}";
    return ret;
}

String StatementGenerator::strAppendAnyValue(RandomGenerator & rg, const bool allow_cast, SQLType * tp)
{
    String ret = tp->appendRandomRawValue(rg, *this);

    if (allow_cast && rg.nextSmallNumber() < 7)
    {
        ret += "::";
        ret += tp->typeName(false, false);
    }
    return ret;
}

}
