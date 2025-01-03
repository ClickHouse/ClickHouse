#pragma once

#include <Client/BuzzHouse/AST/SQLProtoStr.h>
#include <Client/BuzzHouse/Generator/RandomGenerator.h>

#include <memory>
#include <optional>

namespace BuzzHouse
{

const constexpr uint32_t allow_bool = (1 << 0), allow_unsigned_int = (1 << 1), allow_int8 = (1 << 2), allow_int64 = (1 << 3),
                         allow_int128 = (1 << 4), allow_floating_points = (1 << 5), allow_dates = (1 << 6), allow_date32 = (1 << 7),
                         allow_datetimes = (1 << 8), allow_datetime64 = (1 << 9), allow_strings = (1 << 10), allow_decimals = (1 << 11),
                         allow_uuid = (1 << 12), allow_enum = (1 << 13), allow_dynamic = (1 << 14), allow_JSON = (1 << 15),
                         allow_nullable = (1 << 16), allow_low_cardinality = (1 << 17), allow_array = (1 << 18), allow_map = (1 << 19),
                         allow_tuple = (1 << 20), allow_variant = (1 << 21), allow_nested = (1 << 22),
                         allow_nullable_inside_array = (1 << 23), allow_ipv4 = (1 << 24), allow_ipv6 = (1 << 25), allow_geo = (1 << 26),
                         set_any_datetime_precision = (1 << 27);

class SQLType
{
public:
    virtual void typeName(std::string & ret, bool escape) const = 0;
    virtual void MySQLtypeName(RandomGenerator & rg, std::string & ret, bool escape) const = 0;
    virtual void PostgreSQLtypeName(RandomGenerator & rg, std::string & ret, bool escape) const = 0;
    virtual void SQLitetypeName(RandomGenerator & rg, std::string & ret, bool escape) const = 0;

    virtual ~SQLType() = default;
};

SQLType * TypeDeepCopy(SQLType * tp);

class BoolType : public SQLType
{
public:
    void typeName(std::string & ret, const bool) const override { ret += "Bool"; }
    void MySQLtypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "BOOL"; }
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "BOOLEAN"; }
    void SQLitetypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "INTEGER"; }

    ~BoolType() override = default;
};

class IntType : public SQLType
{
public:
    const uint32_t size;
    const bool is_unsigned;
    IntType(const uint32_t s, const bool isu) : size(s), is_unsigned(isu) { }

    void typeName(std::string & ret, const bool) const override
    {
        ret += is_unsigned ? "U" : "";
        ret += "Int";
        ret += std::to_string(size);
    }
    void MySQLtypeName(RandomGenerator &, std::string & ret, const bool) const override
    {
        switch (size)
        {
            case 8:
                ret += "TINYINT";
                break;
            case 16:
                ret += "SMALLINT";
                break;
            case 32:
                ret += "INT";
                break;
            case 64:
                ret += "BIGINT";
                break;
            default:
                assert(0);
        }
        ret += is_unsigned ? " UNSIGNED" : "";
    }
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override
    {
        switch (size)
        {
            case 8:
            case 16:
                ret += "SMALLINT";
                break;
            case 32:
                ret += "INTEGER";
                break;
            case 64:
                ret += "BIGINT";
                break;
            default:
                assert(0);
        }
    }
    void SQLitetypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "INTEGER"; }

    ~IntType() override = default;
};

class FloatType : public SQLType
{
public:
    const uint32_t size;
    explicit FloatType(const uint32_t s) : size(s) { }

    void typeName(std::string & ret, const bool) const override
    {
        if (size == 16)
        {
            ret += "B";
        }
        ret += "Float";
        ret += std::to_string(size);
    }
    void MySQLtypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += (size == 32) ? "FLOAT" : "DOUBLE"; }
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override
    {
        ret += (size == 32) ? "REAL" : "DOUBLE PRECISION";
    }
    void SQLitetypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "REAL"; }

    ~FloatType() override = default;
};

class DateType : public SQLType
{
public:
    const bool extended;
    explicit DateType(const bool ex) : extended(ex) { }

    void typeName(std::string & ret, const bool) const override
    {
        ret += "Date";
        if (extended)
        {
            ret += "32";
        }
    }
    void MySQLtypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "DATE"; }
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "DATE"; }
    void SQLitetypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "TEXT"; }

    ~DateType() override = default;
};

class DateTimeType : public SQLType
{
public:
    const bool extended;
    const std::optional<const uint32_t> precision;
    const std::optional<const std::string> timezone;

    DateTimeType(const bool ex, const std::optional<const uint32_t> p, const std::optional<const std::string> t)
        : extended(ex), precision(p), timezone(t)
    {
    }

    void typeName(std::string & ret, const bool escape) const override
    {
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
    }
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool) const override
    {
        ret += rg.nextBool() ? "DATETIME" : "TIMESTAMP";
    }
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "TIMESTAMP"; }
    void SQLitetypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "TEXT"; }

    ~DateTimeType() override = default;
};

class DecimalType : public SQLType
{
public:
    const std::optional<const uint32_t> precision, scale;
    DecimalType(const std::optional<const uint32_t> p, const std::optional<const uint32_t> s) : precision(p), scale(s) { }

    void typeName(std::string & ret, const bool) const override
    {
        ret += "Decimal";
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
    void MySQLtypeName(RandomGenerator &, std::string & ret, const bool escape) const override
    {
        std::string next;

        typeName(next, escape);
        std::transform(next.begin(), next.end(), next.begin(), [](unsigned char c) { return std::toupper(c); });
        ret += next;
    }
    void PostgreSQLtypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override { MySQLtypeName(rg, ret, escape); }
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override { MySQLtypeName(rg, ret, escape); }

    ~DecimalType() override = default;
};

class StringType : public SQLType
{
public:
    const std::optional<const uint32_t> precision;
    explicit StringType(const std::optional<const uint32_t> p) : precision(p) { }

    void typeName(std::string & ret, const bool) const override
    {
        if (precision.has_value())
        {
            ret += "FixedString(";
            ret += std::to_string(precision.value());
            ret += ")";
        }
        else
        {
            ret += "String";
        }
    }
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool) const override
    {
        if (precision.has_value())
        {
            ret += rg.nextBool() ? "VAR" : "";
            ret += rg.nextBool() ? "CHAR" : "BINARY";
            ret += "(";
            ret += std::to_string(precision.value());
            ret += ")";
        }
        else
        {
            ret += rg.nextBool() ? "BLOB" : "TEXT";
        }
    }
    void PostgreSQLtypeName(RandomGenerator & rg, std::string & ret, const bool) const override
    {
        if (precision.has_value())
        {
            ret += rg.nextBool() ? "VAR" : "";
            ret += "CHAR(";
            ret += std::to_string(precision.value());
            ret += ")";
        }
        else
        {
            ret += "TEXT";
        }
    }
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool) const override { ret += rg.nextBool() ? "BLOB" : "TEXT"; }

    ~StringType() override = default;
};

class UUIDType : public SQLType
{
public:
    void typeName(std::string & ret, const bool) const override { ret += "UUID"; }
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool) const override { ret += rg.nextBool() ? "BLOB" : "TEXT"; }
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool escape) const override { typeName(ret, escape); }
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool) const override { ret += rg.nextBool() ? "BLOB" : "TEXT"; }

    ~UUIDType() override = default;
};

class EnumValue
{
public:
    const std::string val;
    const int32_t number;

    EnumValue(const std::string v, const int32_t n) : val(v), number(n) { }
};

class EnumType : public SQLType
{
public:
    const uint32_t size;
    const std::vector<EnumValue> values;
    EnumType(const uint32_t s, const std::vector<EnumValue> v) : size(s), values(v) { }

    void typeName(std::string & ret, const bool escape) const override
    {
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
                if (escape && c == '\'')
                {
                    ret += "\\";
                }
                ret += c;
            }
            ret += " = ";
            ret += std::to_string(v.number);
        }
        ret += ")";
    }
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool) const override { ret += rg.nextBool() ? "BLOB" : "TEXT"; }
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "TEXT"; }
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool) const override { ret += rg.nextBool() ? "BLOB" : "TEXT"; }

    ~EnumType() override = default;
};

class IPv4Type : public SQLType
{
public:
    void typeName(std::string & ret, const bool) const override { ret += "IPv4"; }
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool) const override { ret += rg.nextBool() ? "BLOB" : "TEXT"; }
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "TEXT"; }
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool) const override { ret += rg.nextBool() ? "BLOB" : "TEXT"; }

    ~IPv4Type() override = default;
};

class IPv6Type : public SQLType
{
public:
    void typeName(std::string & ret, const bool) const override { ret += "IPv6"; }
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool) const override { ret += rg.nextBool() ? "BLOB" : "TEXT"; }
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "TEXT"; }
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool) const override { ret += rg.nextBool() ? "BLOB" : "TEXT"; }

    ~IPv6Type() override = default;
};

class DynamicType : public SQLType
{
public:
    const std::optional<const uint32_t> ntypes;
    explicit DynamicType(const std::optional<const uint32_t> n) : ntypes(n) { }

    void typeName(std::string & ret, const bool) const override
    {
        ret += "Dynamic";
        if (ntypes.has_value())
        {
            ret += "(max_types=";
            ret += std::to_string(ntypes.value());
            ret += ")";
        }
    }
    void MySQLtypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }
    void PostgreSQLtypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }
    void SQLitetypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }

    ~DynamicType() override = default;
};

class JSubType
{
public:
    const std::string cname;
    SQLType * subtype;

    JSubType(const std::string & n, SQLType * s) : cname(n), subtype(s) { }
};

class JSONType : public SQLType
{
public:
    const std::string desc;
    const std::vector<JSubType> subcols;
    explicit JSONType(const std::string & s, const std::vector<JSubType> sc) : desc(s), subcols(sc) { }

    void typeName(std::string & ret, const bool escape) const override
    {
        ret += "JSON";
        for (const auto & c : desc)
        {
            if (escape && c == '\'')
            {
                ret += '\\';
            }
            ret += c;
        }
    }
    void MySQLtypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "JSON"; }
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "JSON"; }
    void SQLitetypeName(RandomGenerator &, std::string & ret, const bool) const override { ret += "TEXT"; }

    ~JSONType() override
    {
        for (const auto & entry : subcols)
        {
            delete entry.subtype;
        }
    }
};

class Nullable : public SQLType
{
public:
    SQLType * subtype;
    explicit Nullable(SQLType * s) : subtype(s) { }

    void typeName(std::string & ret, const bool escape) const override
    {
        ret += "Nullable(";
        subtype->typeName(ret, escape);
        ret += ")";
    }
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        subtype->MySQLtypeName(rg, ret, escape);
    }
    void PostgreSQLtypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        subtype->PostgreSQLtypeName(rg, ret, escape);
    }
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        subtype->SQLitetypeName(rg, ret, escape);
    }

    ~Nullable() override { delete subtype; }
};

class LowCardinality : public SQLType
{
public:
    SQLType * subtype;
    explicit LowCardinality(SQLType * s) : subtype(s) { }

    void typeName(std::string & ret, const bool escape) const override
    {
        ret += "LowCardinality(";
        subtype->typeName(ret, escape);
        ret += ")";
    }
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        subtype->MySQLtypeName(rg, ret, escape);
    }
    void PostgreSQLtypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        subtype->PostgreSQLtypeName(rg, ret, escape);
    }
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        subtype->SQLitetypeName(rg, ret, escape);
    }

    ~LowCardinality() override { delete subtype; }
};

class GeoType : public SQLType
{
public:
    const GeoTypes geo_type;
    explicit GeoType(const GeoTypes & gt) : geo_type(gt) { }

    void typeName(std::string & ret, const bool) const override { ret += GeoTypes_Name(geo_type); }
    void MySQLtypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }
    void PostgreSQLtypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }
    void SQLitetypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }

    ~GeoType() override = default;
};

class ArrayType : public SQLType
{
public:
    SQLType * subtype;
    explicit ArrayType(SQLType * s) : subtype(s) { }

    void typeName(std::string & ret, const bool escape) const override
    {
        ret += "Array(";
        subtype->typeName(ret, escape);
        ret += ")";
    }
    void MySQLtypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }
    void PostgreSQLtypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
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
        nsubtype->PostgreSQLtypeName(rg, ret, escape);
        ret += "[]";
    }
    void SQLitetypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }

    ~ArrayType() override { delete subtype; }
};

class MapType : public SQLType
{
public:
    SQLType *key, *value;
    MapType(SQLType * k, SQLType * v) : key(k), value(v) { }

    void typeName(std::string & ret, const bool escape) const override
    {
        ret += "Map(";
        key->typeName(ret, escape);
        ret += ",";
        value->typeName(ret, escape);
        ret += ")";
    }
    void MySQLtypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }
    void PostgreSQLtypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }
    void SQLitetypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }

    ~MapType() override
    {
        delete key;
        delete value;
    }
};

class SubType
{
public:
    const std::optional<const uint32_t> cname;
    SQLType * subtype;

    SubType(const std::optional<const uint32_t> n, SQLType * s) : cname(n), subtype(s) { }
};

class TupleType : public SQLType
{
public:
    const std::vector<SubType> subtypes;
    explicit TupleType(const std::vector<SubType> s) : subtypes(s) { }

    void typeName(std::string & ret, const bool escape) const override
    {
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
            sub.subtype->typeName(ret, escape);
        }
        ret += ")";
    }
    void MySQLtypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }
    void PostgreSQLtypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }
    void SQLitetypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }

    ~TupleType() override
    {
        for (const auto & entry : subtypes)
        {
            delete entry.subtype;
        }
    }
};

class VariantType : public SQLType
{
public:
    const std::vector<SQLType *> subtypes;
    explicit VariantType(const std::vector<SQLType *> s) : subtypes(s) { }

    void typeName(std::string & ret, const bool escape) const override
    {
        ret += "Variant(";
        for (size_t i = 0; i < subtypes.size(); i++)
        {
            if (i != 0)
            {
                ret += ",";
            }
            subtypes[i]->typeName(ret, escape);
        }
        ret += ")";
    }
    void MySQLtypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }
    void PostgreSQLtypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }
    void SQLitetypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }

    ~VariantType() override
    {
        for (const auto & entry : subtypes)
        {
            delete entry;
        }
    }
};

class NestedSubType
{
public:
    uint32_t cname;
    SQLType * subtype;
    ArrayType * array_subtype;

    NestedSubType(const uint32_t n, SQLType * s) : cname(n), subtype(s), array_subtype(new ArrayType(TypeDeepCopy(s))) { }
};

class NestedType : public SQLType
{
public:
    std::vector<NestedSubType> subtypes;
    explicit NestedType(std::vector<NestedSubType> s) : subtypes(s) { }

    void typeName(std::string & ret, const bool escape) const override
    {
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
            sub.subtype->typeName(ret, escape);
        }
        ret += ")";
    }
    void MySQLtypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }
    void PostgreSQLtypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }
    void SQLitetypeName(RandomGenerator &, std::string &, const bool) const override { assert(0); }

    ~NestedType() override
    {
        for (const auto & entry : subtypes)
        {
            delete entry.array_subtype;
            delete entry.subtype;
        }
    }
};

template <typename T, bool SArray, bool SNullable, bool SNested>
bool hasType(SQLType * tp)
{
    LowCardinality * lc;

    if (dynamic_cast<const T *>(tp))
    {
        return true;
    }
    if constexpr (SNullable)
    {
        Nullable * nl;

        if ((nl = dynamic_cast<Nullable *>(tp)))
        {
            return hasType<T, SArray, SNullable, SNested>(nl->subtype);
        }
    }
    if ((lc = dynamic_cast<LowCardinality *>(tp)))
    {
        return hasType<T, SArray, SNullable, SNested>(lc->subtype);
    }
    if constexpr (SArray)
    {
        ArrayType * at;

        if ((at = dynamic_cast<ArrayType *>(tp)))
        {
            return hasType<T, SArray, SNullable, SNested>(at->subtype);
        }
    }
    if constexpr (SNested)
    {
        TupleType * ttp;
        NestedType * ntp;

        if ((ttp = dynamic_cast<TupleType *>(tp)))
        {
            for (const auto & entry : ttp->subtypes)
            {
                if (hasType<T, SArray, SNullable, SNested>(entry.subtype))
                {
                    return true;
                }
            }
        }
        else if ((ntp = dynamic_cast<NestedType *>(tp)))
        {
            for (const auto & entry : ntp->subtypes)
            {
                if (hasType<T, SArray, SNullable, SNested>(entry.subtype))
                {
                    return true;
                }
            }
        }
    }
    return false;
}

void appendDecimal(RandomGenerator & rg, std::string & ret, uint32_t left, uint32_t right);
void strBuildJSONArray(RandomGenerator & rg, int jdepth, int jwidth, std::string & ret);
void strBuildJSONElement(RandomGenerator & rg, std::string & ret);
void strBuildJSON(RandomGenerator & rg, int jdepth, int jwidth, std::string & ret);
void strAppendGeoValue(RandomGenerator & rg, std::string & ret, const GeoTypes & geo_type);
}
