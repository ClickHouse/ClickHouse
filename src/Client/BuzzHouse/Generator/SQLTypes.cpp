#include <cstdint>

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

String BoolType::typeName(const bool) const
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

SQLType * BoolType::typeDeepCopy() const
{
    return new BoolType();
}

String BoolType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return rg.nextBool() ? "TRUE" : "FALSE";
}

String IntType::typeName(const bool) const
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

SQLType * IntType::typeDeepCopy() const
{
    return new IntType(size, is_unsigned);
}

String IntType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
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

String FloatType::typeName(const bool) const
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

SQLType * FloatType::typeDeepCopy() const
{
    return new FloatType(size);
}

String FloatType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return nextFloatingPoint(rg, true);
}

String DateType::typeName(const bool) const
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

SQLType * DateType::typeDeepCopy() const
{
    return new DateType(extended);
}

String DateType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return "'" + (extended ? rg.nextDate32() : rg.nextDate()) + "'";
}

String TimeType::typeName(const bool) const
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

SQLType * TimeType::typeDeepCopy() const
{
    return new TimeType(extended, precision);
}

String TimeType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return "'" + (extended ? rg.nextTime64(precision.has_value()) : rg.nextTime()) + "'";
}

String DateTimeType::typeName(const bool escape) const
{
    String ret;

    ret += "DateTime";
    if (extended)
    {
        ret += "64";
    }
    if (precision.has_value() || timezone.has_value())
    {
        ret += "(";
        if (precision.has_value())
        {
            ret += std::to_string(precision.value());
        }
        if (timezone.has_value())
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

SQLType * DateTimeType::typeDeepCopy() const
{
    return new DateTimeType(extended, precision, timezone);
}

String DateTimeType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return "'" + (extended ? rg.nextDateTime64(rg.nextSmallNumber() < 8) : rg.nextDateTime(precision.has_value())) + "'";
}

String DecimalType::typeName(const bool) const
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

SQLType * DecimalType::typeDeepCopy() const
{
    return new DecimalType(short_notation, precision, scale);
}

String DecimalType::appendDecimalValue(RandomGenerator & rg, const bool use_func, const DecimalType * dt)
{
    const uint32_t right = dt->scale.value_or(0);
    const uint32_t left = dt->precision.value_or(10) - right;

    return appendDecimal(rg, use_func, left, right);
}

String DecimalType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return appendDecimalValue(rg, true, this);
}

String StringType::typeName(const bool) const
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

SQLType * StringType::typeDeepCopy() const
{
    return new StringType(precision);
}

String StringType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return rg.nextString("'", true, precision.value_or(rg.nextStrlen()));
}

String UUIDType::typeName(const bool) const
{
    return "UUID";
}

String UUIDType::MySQLtypeName(RandomGenerator & rg, const bool) const
{
    return rg.nextBool() ? "BLOB" : "TEXT";
}

String UUIDType::PostgreSQLtypeName(RandomGenerator &, const bool escape) const
{
    return typeName(escape);
}

String UUIDType::SQLitetypeName(RandomGenerator & rg, const bool) const
{
    return rg.nextBool() ? "BLOB" : "TEXT";
}

SQLType * UUIDType::typeDeepCopy() const
{
    return new UUIDType();
}

String UUIDType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return "'" + rg.nextUUID() + "'";
}

String EnumType::typeName(const bool escape) const
{
    String ret;

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

SQLType * EnumType::typeDeepCopy() const
{
    return new EnumType(size, values);
}

String EnumType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return rg.pickRandomly(values).val;
}

String IPv4Type::typeName(const bool) const
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

SQLType * IPv4Type::typeDeepCopy() const
{
    return new IPv4Type();
}

String IPv4Type::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return "'" + rg.nextIPv4() + "'";
}

String IPv6Type::typeName(const bool) const
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

SQLType * IPv6Type::typeDeepCopy() const
{
    return new IPv6Type();
}

String IPv6Type::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return "'" + rg.nextIPv6() + "'";
}

String DynamicType::typeName(const bool) const
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

SQLType * DynamicType::typeDeepCopy() const
{
    return new DynamicType(ntypes);
}

String DynamicType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    uint32_t col_counter = 0;
    const uint32_t type_mask_backup = gen.next_type_mask;

    gen.next_type_mask = gen.fc.type_mask & ~(allow_dynamic | allow_nested);
    auto next = std::unique_ptr<SQLType>(gen.randomNextType(rg, gen.next_type_mask, col_counter, nullptr));
    gen.next_type_mask = type_mask_backup;
    String ret = next->appendRandomRawValue(rg, gen);

    if (rg.nextMediumNumber() < 4)
    {
        ret += "::";
        ret += next->typeName(false);
    }
    return ret;
}

String JSONType::typeName(const bool escape) const
{
    String ret;

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

SQLType * JSONType::typeDeepCopy() const
{
    std::vector<JSubType> jsubcols;

    jsubcols.reserve(subcols.size());
    for (const auto & entry : subcols)
    {
        jsubcols.emplace_back(JSubType(entry.cname, entry.subtype->typeDeepCopy()));
    }
    return new JSONType(desc, std::move(jsubcols));
}

String JSONType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    std::uniform_int_distribution<int> dopt(1, gen.fc.max_depth);
    std::uniform_int_distribution<int> wopt(1, gen.fc.max_width);

    return "'" + strBuildJSON(rg, dopt(rg.generator), wopt(rg.generator)) + "'";
}

JSONType::~JSONType()
{
    for (const auto & entry : subcols)
    {
        delete entry.subtype;
    }
}

String Nullable::typeName(const bool escape) const
{
    return fmt::format("Nullable({})", subtype->typeName(escape));
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

SQLType * Nullable::typeDeepCopy() const
{
    return new Nullable(subtype->typeDeepCopy());
}

String Nullable::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    return rg.nextMediumNumber() < 6 ? "NULL" : subtype->appendRandomRawValue(rg, gen);
}

String LowCardinality::typeName(const bool escape) const
{
    return fmt::format("LowCardinality({})", subtype->typeName(escape));
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

SQLType * LowCardinality::typeDeepCopy() const
{
    return new LowCardinality(subtype->typeDeepCopy());
}

String LowCardinality::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    return subtype->appendRandomRawValue(rg, gen);
}

String GeoType::typeName(const bool) const
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

SQLType * GeoType::typeDeepCopy() const
{
    return new GeoType(geotype);
}

String GeoType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator &) const
{
    return strAppendGeoValue(rg, geotype);
}

String ArrayType::typeName(const bool escape) const
{
    return fmt::format("Array({})", subtype->typeName(escape));
}

String ArrayType::MySQLtypeName(RandomGenerator &, const bool) const
{
    return "TEXT []";
}

String ArrayType::PostgreSQLtypeName(RandomGenerator & rg, const bool escape) const
{
    SQLType * nsubtype = subtype;
    Nullable * nl = nullptr;
    LowCardinality * lc = nullptr;

    while (true)
    {
        if ((nl = dynamic_cast<Nullable *>(nsubtype)))
        {
            nsubtype = nl->subtype;
        }
        else if ((lc = dynamic_cast<LowCardinality *>(nsubtype)))
        {
            nsubtype = lc->subtype;
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

SQLType * ArrayType::typeDeepCopy() const
{
    return new ArrayType(subtype->typeDeepCopy());
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

    return appendRandomRawValue(rg, gen, subtype, rows_dist(rg.generator));
}

String MapType::typeName(const bool escape) const
{
    return fmt::format("Map({},{})", key->typeName(escape), value->typeName(escape));
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

SQLType * MapType::typeDeepCopy() const
{
    return new MapType(key->typeDeepCopy(), value->typeDeepCopy());
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

MapType::~MapType()
{
    delete key;
    delete value;
}

String TupleType::typeName(const bool escape) const
{
    String ret;

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
        ret += sub.subtype->typeName(escape);
    }
    ret += ")";
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

SQLType * TupleType::typeDeepCopy() const
{
    std::vector<SubType> nsubtypes;

    nsubtypes.reserve(subtypes.size());
    for (const auto & entry : subtypes)
    {
        nsubtypes.emplace_back(SubType(entry.cname, entry.subtype->typeDeepCopy()));
    }
    return new TupleType(std::move(nsubtypes));
}

String TupleType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    String ret = "(";
    for (const auto & entry : subtypes)
    {
        ret += entry.subtype->appendRandomRawValue(rg, gen);
        ret += ", ";
    }
    ret += ")";
    return ret;
}

TupleType::~TupleType()
{
    for (const auto & entry : subtypes)
    {
        delete entry.subtype;
    }
}

String VariantType::typeName(const bool escape) const
{
    String ret;

    ret += "Variant(";
    for (size_t i = 0; i < subtypes.size(); i++)
    {
        if (i != 0)
        {
            ret += ",";
        }
        ret += subtypes[i]->typeName(escape);
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

SQLType * VariantType::typeDeepCopy() const
{
    std::vector<SQLType *> nsubtypes;

    nsubtypes.reserve(subtypes.size());
    for (const auto & entry : subtypes)
    {
        nsubtypes.emplace_back(entry->typeDeepCopy());
    }
    return new VariantType(std::move(nsubtypes));
}

String VariantType::appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const
{
    return subtypes.empty() ? "NULL" : rg.pickRandomly(subtypes)->appendRandomRawValue(rg, gen);
}

VariantType::~VariantType()
{
    for (const auto & entry : subtypes)
    {
        delete entry;
    }
}

String NestedType::typeName(const bool escape) const
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
        ret += sub.subtype->typeName(escape);
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

SQLType * NestedType::typeDeepCopy() const
{
    std::vector<NestedSubType> nsubtypes;

    nsubtypes.reserve(subtypes.size());
    for (const auto & entry : subtypes)
    {
        nsubtypes.emplace_back(NestedSubType(entry.cname, entry.subtype->typeDeepCopy()));
    }
    return new NestedType(std::move(nsubtypes));
}

NestedType::~NestedType()
{
    for (const auto & entry : subtypes)
    {
        delete entry.subtype;
    }
}

std::tuple<SQLType *, Integers> StatementGenerator::randomIntType(RandomGenerator & rg, const uint32_t allowed_types)
{
    chassert(this->ids.empty());

    if ((allowed_types & allow_unsigned_int))
    {
        if ((allowed_types & allow_int8))
        {
            this->ids.emplace_back(1);
        }
        this->ids.emplace_back(2);
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
    this->ids.emplace_back(8);
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
            return std::make_tuple(new IntType(8, true), Integers::UInt8);
        case 2:
            return std::make_tuple(new IntType(16, true), Integers::UInt16);
        case 3:
            return std::make_tuple(new IntType(32, true), Integers::UInt32);
        case 4:
            return std::make_tuple(new IntType(64, true), Integers::UInt64);
        case 5:
            return std::make_tuple(new IntType(128, true), Integers::UInt128);
        case 6:
            return std::make_tuple(new IntType(256, true), Integers::UInt256);
        case 7:
            return std::make_tuple(new IntType(8, false), Integers::Int8);
        case 8:
            return std::make_tuple(new IntType(16, false), Integers::Int16);
        case 9:
            return std::make_tuple(new IntType(32, false), Integers::Int32);
        case 10:
            return std::make_tuple(new IntType(64, false), Integers::Int64);
        case 11:
            return std::make_tuple(new IntType(128, false), Integers::Int128);
        case 12:
            return std::make_tuple(new IntType(256, false), Integers::Int256);
        default:
            chassert(0);
    }
    return std::make_tuple(new IntType(32, false), Integers::Int32);
}

std::tuple<SQLType *, FloatingPoints> StatementGenerator::randomFloatType(RandomGenerator & rg) const
{
    const uint32_t nopt = (rg.nextSmallNumber() % 3) + 1;
    return std::make_tuple(new FloatType(1 << (nopt + 3)), static_cast<FloatingPoints>(nopt));
}

std::tuple<SQLType *, Dates> StatementGenerator::randomDateType(RandomGenerator & rg, const uint32_t allowed_types) const
{
    const bool use32 = (allowed_types & allow_date32) && rg.nextBool();
    return std::make_tuple(new DateType(use32), use32 ? Dates::Date32 : Dates::Date);
}

SQLType * StatementGenerator::randomTimeType(RandomGenerator & rg, const uint32_t allowed_types, TimeTp * dt) const
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
    return new TimeType(use64, precision);
}

SQLType * StatementGenerator::randomDateTimeType(RandomGenerator & rg, const uint32_t allowed_types, DateTimeTp * dt) const
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
    return new DateTimeType(use64, precision, timezone);
}

SQLType * StatementGenerator::randomDecimalType(RandomGenerator & rg, const uint32_t allowed_types, BottomTypeName * tp) const
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
        scale = std::optional<uint32_t>(rg.nextRandomUInt32() % (precision.value() + 1));
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
            precision = std::optional<uint32_t>((rg.nextRandomUInt32() % ((allowed_types & set_no_decimal_limit) ? 76 : 65)) + 1);
            if (dec)
            {
                ds->set_precision(precision.value());
            }
            if (rg.nextBool())
            {
                scale = std::optional<uint32_t>(rg.nextRandomUInt32() % (precision.value() + 1));
                if (dec)
                {
                    ds->set_scale(scale.value());
                }
            }
        }
    }
    return new DecimalType(short_notation, precision, scale);
}

SQLType * StatementGenerator::bottomType(RandomGenerator & rg, const uint32_t allowed_types, const bool low_card, BottomTypeName * tp)
{
    SQLType * res = nullptr;

    const uint32_t int_type = 40;
    const uint32_t floating_point_type
        = 10 * static_cast<uint32_t>((allowed_types & allow_floating_points) != 0 && this->fc.fuzz_floating_points);
    const uint32_t date_type = 15 * static_cast<uint32_t>((allowed_types & allow_dates) != 0);
    const uint32_t datetime_type = 15 * static_cast<uint32_t>((allowed_types & allow_datetimes) != 0);
    const uint32_t string_type = 30 * static_cast<uint32_t>((allowed_types & allow_strings) != 0);
    const uint32_t decimal_type = 20 * static_cast<uint32_t>((allowed_types & allow_decimals) != 0);
    const uint32_t bool_type = 20 * static_cast<uint32_t>((allowed_types & allow_bool) != 0);
    const uint32_t enum_type = 20 * static_cast<uint32_t>(!low_card && (allowed_types & allow_enum) != 0);
    const uint32_t uuid_type = 10 * static_cast<uint32_t>((allowed_types & allow_uuid) != 0);
    const uint32_t ipv4_type = 5 * static_cast<uint32_t>((allowed_types & allow_ipv4) != 0);
    const uint32_t ipv6_type = 5 * static_cast<uint32_t>((allowed_types & allow_ipv6) != 0);
    const uint32_t j_type = 20 * static_cast<uint32_t>(!low_card && (allowed_types & allow_JSON) != 0);
    const uint32_t dynamic_type = 30 * static_cast<uint32_t>(!low_card && (allowed_types & allow_dynamic) != 0);
    const uint32_t time_type = 15 * static_cast<uint32_t>((allowed_types & allow_time) != 0);
    const uint32_t prob_space = int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type
        + enum_type + uuid_type + ipv4_type + ipv6_type + j_type + dynamic_type + time_type;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    if (nopt < (int_type + 1))
    {
        Integers nint;

        std::tie(res, nint) = randomIntType(rg, allowed_types);
        if (tp)
        {
            tp->set_integers(nint);
        }
    }
    else if (floating_point_type && nopt < (int_type + floating_point_type + 1))
    {
        FloatingPoints nflo;

        std::tie(res, nflo) = randomFloatType(rg);
        if (tp)
        {
            tp->set_floats(nflo);
        }
    }
    else if (date_type && nopt < (int_type + floating_point_type + date_type + 1))
    {
        Dates dd;

        std::tie(res, dd) = randomDateType(rg, allowed_types);
        if (tp)
        {
            tp->set_dates(dd);
        }
    }
    else if (datetime_type && nopt < (int_type + floating_point_type + date_type + datetime_type + 1))
    {
        DateTimeTp * dtp = tp ? tp->mutable_datetimes() : nullptr;

        res = randomDateTimeType(rg, low_card ? (allowed_types & ~(allow_datetime64)) : allowed_types, dtp);
    }
    else if (string_type && nopt < (int_type + floating_point_type + date_type + datetime_type + string_type + 1))
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
        res = new StringType(swidth);
    }
    else if (decimal_type && nopt < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + 1))
    {
        res = randomDecimalType(rg, allowed_types, tp);
    }
    else if (bool_type && nopt < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + 1))
    {
        if (tp)
        {
            tp->set_boolean(true);
        }
        res = new BoolType();
    }
    else if (
        enum_type
        && nopt < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + enum_type + 1))
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
        res = new EnumType(bits16 ? 16 : 8, evs);
    }
    else if (
        uuid_type
        && nopt
            < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + enum_type + uuid_type
               + 1))
    {
        if (tp)
        {
            tp->set_uuid(true);
        }
        res = new UUIDType();
    }
    else if (
        ipv4_type
        && nopt
            < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + enum_type + uuid_type
               + ipv4_type + 1))
    {
        if (tp)
        {
            tp->set_ipv4(true);
        }
        res = new IPv4Type();
    }
    else if (
        ipv6_type
        && nopt
            < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + enum_type + uuid_type
               + ipv4_type + ipv6_type + 1))
    {
        if (tp)
        {
            tp->set_ipv6(true);
        }
        res = new IPv6Type();
    }
    else if (
        j_type
        && nopt
            < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + enum_type + uuid_type
               + ipv4_type + ipv6_type + j_type + 1))
    {
        String desc;
        std::vector<JSubType> subcols;
        JSONDef * jdef = tp ? tp->mutable_jdef() : nullptr;
        const uint32_t nclauses = rg.nextLargeNumber() % 7;

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
                const uint32_t max_dpaths = rg.nextBool() ? (rg.nextMediumNumber() % 5) : (rg.nextRandomUInt32() % 1025);

                if (tp)
                {
                    jdi->set_max_dynamic_paths(max_dpaths);
                }
                desc += "max_dynamic_paths=";
                desc += std::to_string(max_dpaths);
            }
            else if (this->depth >= this->fc.max_depth || noption < 8)
            {
                const uint32_t max_dtypes = rg.nextBool() ? (rg.nextMediumNumber() % 5) : (rg.nextRandomUInt32() % 33);

                if (tp)
                {
                    jdi->set_max_dynamic_types(max_dtypes);
                }
                desc += "max_dynamic_types=";
                desc += std::to_string(max_dtypes);
            }
            else
            {
                uint32_t col_counter = 0;
                const uint32_t ncols = (rg.nextMediumNumber() % 4) + 1;
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

                const uint32_t type_mask_backup = this->next_type_mask;
                this->next_type_mask = fc.type_mask & ~(allow_nested | allow_enum);
                SQLType * jtp = randomNextType(rg, this->next_type_mask, col_counter, tp ? jpt->mutable_type() : nullptr);
                this->next_type_mask = type_mask_backup;

                desc += jtp->typeName(false);
                subcols.emplace_back(JSubType(npath, jtp));
            }
        }
        this->depth--;
        if (nclauses)
        {
            desc += ")";
        }
        res = new JSONType(desc, subcols);
    }
    else if (
        dynamic_type
        && nopt
            < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + enum_type + uuid_type
               + ipv4_type + ipv6_type + j_type + dynamic_type + 1))
    {
        Dynamic * dyn = tp ? tp->mutable_dynamic() : nullptr;
        std::optional<uint32_t> ntypes;

        if (rg.nextBool())
        {
            ntypes = std::optional<uint32_t>(rg.nextBool() ? rg.nextSmallNumber() : ((rg.nextRandomUInt32() % 100) + 1));
            if (dyn)
            {
                dyn->set_ntypes(ntypes.value());
            }
        }
        res = new DynamicType(ntypes);
    }
    else if (
        time_type
        && nopt
            < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + enum_type + uuid_type
               + ipv4_type + ipv6_type + j_type + dynamic_type + time_type + 1))
    {
        TimeTp * tt = tp ? tp->mutable_times() : nullptr;

        res = randomTimeType(rg, low_card ? (allowed_types & ~(allow_time64)) : allowed_types, tt);
    }
    else
    {
        chassert(0);
    }
    return res;
}

SQLType * StatementGenerator::randomNextType(RandomGenerator & rg, const uint32_t allowed_types, uint32_t & col_counter, TopTypeName * tp)
{
    const uint32_t non_nullable_type = 60;
    const uint32_t nullable_type = 25 * static_cast<uint32_t>((allowed_types & allow_nullable) != 0);
    const uint32_t array_type = 10 * static_cast<uint32_t>((allowed_types & allow_array) != 0 && this->depth < this->fc.max_depth);
    const uint32_t map_type = 10
        * static_cast<uint32_t>((allowed_types & allow_map) != 0 && this->depth < this->fc.max_depth && this->width < this->fc.max_width);
    const uint32_t tuple_type = 10 * static_cast<uint32_t>((allowed_types & allow_tuple) != 0 && this->depth < this->fc.max_depth);
    const uint32_t variant_type = 10 * static_cast<uint32_t>((allowed_types & allow_variant) != 0 && this->depth < this->fc.max_depth);
    const uint32_t nested_type = 10
        * static_cast<uint32_t>((allowed_types & allow_nested) != 0 && this->depth < this->fc.max_depth
                                && this->width < this->fc.max_width);
    const uint32_t geo_type = 10 * static_cast<uint32_t>((allowed_types & allow_geo) != 0 && this->fc.fuzz_floating_points);
    const uint32_t prob_space
        = nullable_type + non_nullable_type + array_type + map_type + tuple_type + variant_type + nested_type + geo_type;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    if (non_nullable_type && nopt < (non_nullable_type + 1))
    {
        /// Non nullable
        const bool lcard = (allowed_types & allow_low_cardinality) != 0 && rg.nextMediumNumber() < 18;
        SQLType * res
            = bottomType(rg, allowed_types, lcard, tp ? (lcard ? tp->mutable_non_nullable_lcard() : tp->mutable_non_nullable()) : nullptr);
        return lcard ? new LowCardinality(res) : res;
    }
    else if (nullable_type && nopt < (non_nullable_type + nullable_type + 1))
    {
        /// Nullable
        const bool lcard = (allowed_types & allow_low_cardinality) != 0 && rg.nextMediumNumber() < 18;
        SQLType * res = new Nullable(bottomType(
            rg, allowed_types & ~(allow_dynamic), lcard, tp ? (lcard ? tp->mutable_nullable_lcard() : tp->mutable_nullable()) : nullptr));
        return lcard ? new LowCardinality(res) : res;
    }
    else if (array_type && nopt < (nullable_type + non_nullable_type + array_type + 1))
    {
        /// Array
        TopTypeName * arr = tp ? tp->mutable_array() : nullptr;

        this->depth++;
        SQLType * k = this->randomNextType(rg, this->next_type_mask & ~(allow_nested), col_counter, arr);
        this->depth--;
        return new ArrayType(k);
    }
    else if (map_type && nopt < (nullable_type + non_nullable_type + array_type + map_type + 1))
    {
        /// Map
        MapTypeDef * mt = tp ? tp->mutable_map() : nullptr;

        this->depth++;
        SQLType * k = this->randomNextType(
            rg, this->next_type_mask & ~(allow_nullable | allow_nested), col_counter, mt ? mt->mutable_key() : nullptr);
        this->width++;
        SQLType * v = this->randomNextType(rg, this->next_type_mask & ~(allow_nested), col_counter, mt ? mt->mutable_value() : nullptr);
        this->depth--;
        this->width--;
        return new MapType(k, v);
    }
    else if (tuple_type && nopt < (nullable_type + non_nullable_type + array_type + map_type + tuple_type + 1))
    {
        /// Tuple
        std::vector<SubType> subtypes;
        const bool with_names = rg.nextBool();
        TupleTypeDef * tt = tp ? tp->mutable_tuple() : nullptr;
        TupleWithColumnNames * twcn = (tp && with_names) ? tt->mutable_with_names() : nullptr;
        TupleWithOutColumnNames * twocn = (tp && !with_names) ? tt->mutable_no_names() : nullptr;
        const uint32_t ncols
            = this->width >= this->fc.max_width ? 0 : (rg.nextMediumNumber() % std::min<uint32_t>(5, this->fc.max_width - this->width));

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
            SQLType * k
                = this->randomNextType(rg, this->next_type_mask & ~(allow_nested), col_counter, tcd ? tcd->mutable_type_name() : ttn);
            subtypes.emplace_back(SubType(opt_cname, k));
        }
        this->depth--;
        return new TupleType(subtypes);
    }
    else if (variant_type && nopt < (nullable_type + non_nullable_type + array_type + map_type + tuple_type + variant_type + 1))
    {
        /// Variant
        std::vector<SQLType *> subtypes;
        TupleWithOutColumnNames * twocn = tp ? tp->mutable_variant() : nullptr;
        const uint32_t ncols
            = (this->width >= this->fc.max_width ? 0 : (rg.nextMediumNumber() % std::min<uint32_t>(5, this->fc.max_width - this->width)))
            + UINT32_C(1);

        this->depth++;
        for (uint32_t i = 0; i < ncols; i++)
        {
            TopTypeName * ttn = tp ? twocn->add_values() : nullptr;

            subtypes.emplace_back(this->randomNextType(
                rg, this->next_type_mask & ~(allow_nullable | allow_nested | allow_variant | allow_dynamic), col_counter, ttn));
        }
        this->depth--;
        return new VariantType(subtypes);
    }
    else if (
        nested_type && nopt < (nullable_type + non_nullable_type + array_type + map_type + tuple_type + variant_type + nested_type + 1))
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
            SQLType * k
                = this->randomNextType(rg, this->next_type_mask & ~(allow_nested), col_counter, tcd ? tcd->mutable_type_name() : nullptr);
            subtypes.emplace_back(NestedSubType(cname, k));
        }
        this->depth--;
        return new NestedType(subtypes);
    }
    else if (
        geo_type
        && nopt < (nullable_type + non_nullable_type + array_type + map_type + tuple_type + variant_type + nested_type + geo_type + 1))
    {
        /// Geo
        std::uniform_int_distribution<uint32_t> geo_range(1, static_cast<uint32_t>(GeoTypes_MAX));
        const GeoTypes gt = static_cast<GeoTypes>(geo_range(rg.generator));

        if (tp)
        {
            tp->set_geo(gt);
        }
        return new GeoType(gt);
    }
    else
    {
        chassert(0);
    }
    return nullptr;
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
            chassert(0);
        }
        ret += "('";
    }
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
    if (use_func)
    {
        ret += fmt::format("', {})", right);
    }
    return ret;
}

String strAppendGeoValue(RandomGenerator & rg, const GeoTypes & gt)
{
    String ret;
    const uint32_t limit = rg.nextLargeNumber() % 10;

    switch (gt)
    {
        case GeoTypes::Point:
            ret = fmt::format("({},{})", nextFloatingPoint(rg, false), nextFloatingPoint(rg, false));
            break;
        case GeoTypes::Ring:
        case GeoTypes::LineString:
            ret += "[";
            for (uint32_t i = 0; i < limit; i++)
            {
                if (i != 0)
                {
                    ret += ", ";
                }
                ret += fmt::format("({},{})", nextFloatingPoint(rg, false), nextFloatingPoint(rg, false));
            }
            ret += "]";
            break;
        case GeoTypes::MultiLineString:
        case GeoTypes::Polygon:
            ret += "[";
            for (uint32_t i = 0; i < limit; i++)
            {
                const uint32_t nlines = rg.nextLargeNumber() % 10;

                if (i != 0)
                {
                    ret += ", ";
                }
                ret += "[";
                for (uint32_t j = 0; j < nlines; j++)
                {
                    if (j != 0)
                    {
                        ret += ", ";
                    }
                    ret += fmt::format("({},{})", nextFloatingPoint(rg, false), nextFloatingPoint(rg, false));
                }
                ret += "]";
            }
            ret += "]";
            break;
        case GeoTypes::MultiPolygon:
            ret += "[";
            for (uint32_t i = 0; i < limit; i++)
            {
                const uint32_t npoligons = rg.nextLargeNumber() % 10;

                if (i != 0)
                {
                    ret += ", ";
                }
                ret += "[";
                for (uint32_t j = 0; j < npoligons; j++)
                {
                    const uint32_t nlines = rg.nextLargeNumber() % 10;

                    if (j != 0)
                    {
                        ret += ", ";
                    }
                    ret += "[";
                    for (uint32_t k = 0; k < nlines; k++)
                    {
                        if (k != 0)
                        {
                            ret += ", ";
                        }
                        ret += fmt::format("({},{})", nextFloatingPoint(rg, false), nextFloatingPoint(rg, false));
                    }
                    ret += "]";
                }
                ret += "]";
            }
            ret += "]";
            break;
    }
    return ret;
}

String strBuildJSONArray(RandomGenerator & rg, const int jdepth, const int jwidth)
{
    std::uniform_int_distribution<int> jopt(1, 3);
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
                default:
                    chassert(0);
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
    std::uniform_int_distribution<int> opts(1, 22);

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
            ret = '"' + rg.nextDate() + '"';
            break;
        case 15:
            /// Date32
            ret = '"' + rg.nextDate32() + '"';
            break;
        case 16:
            /// Time
            ret = '"' + rg.nextTime() + '"';
            break;
        case 17:
            /// Time64
            ret = '"' + rg.nextTime64(rg.nextSmallNumber() < 8) + '"';
            break;
        case 18:
            /// Datetime
            ret = '"' + rg.nextDateTime(rg.nextSmallNumber() < 8) + '"';
            break;
        case 19:
            /// Datetime64
            ret = '"' + rg.nextDateTime64(rg.nextSmallNumber() < 8) + '"';
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
        default:
            chassert(0);
    }
    return ret;
}

String strBuildJSON(RandomGenerator & rg, const int jdepth, const int jwidth)
{
    String ret = "{";

    if (jdepth && jwidth && rg.nextSmallNumber() < 9)
    {
        std::uniform_int_distribution<int> childd(1, jwidth);
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
                    chassert(0);
            }
        }
    }
    ret += "}";
    return ret;
}

String StatementGenerator::strAppendAnyValue(RandomGenerator & rg, SQLType * tp)
{
    String ret = tp->appendRandomRawValue(rg, *this);

    if (rg.nextSmallNumber() < 7)
    {
        ret += "::";
        ret += tp->typeName(false);
    }
    return ret;
}

}
