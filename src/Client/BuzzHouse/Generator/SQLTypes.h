#pragma once

#include <Client/BuzzHouse/AST/SQLProtoStr.h>
#include <Client/BuzzHouse/Generator/RandomGenerator.h>

#include <optional>

namespace BuzzHouse
{

class SQLType
{
public:
    virtual String typeName(bool escape) const = 0;
    virtual String MySQLtypeName(RandomGenerator & rg, bool escape) const = 0;
    virtual String PostgreSQLtypeName(RandomGenerator & rg, bool escape) const = 0;
    virtual String SQLitetypeName(RandomGenerator & rg, bool escape) const = 0;

    virtual ~SQLType() = default;
};

SQLType * typeDeepCopy(SQLType * tp);

class BoolType : public SQLType
{
public:
    String typeName(bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;

    ~BoolType() override = default;
};

class IntType : public SQLType
{
public:
    const uint32_t size;
    const bool is_unsigned;
    IntType(const uint32_t s, bool isu) : size(s), is_unsigned(isu) { }

    String typeName(bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;

    ~IntType() override = default;
};

class FloatType : public SQLType
{
public:
    const uint32_t size;
    explicit FloatType(const uint32_t s) : size(s) { }

    String typeName(bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;

    ~FloatType() override = default;
};

class DateType : public SQLType
{
public:
    const bool extended;
    explicit DateType(const bool ex) : extended(ex) { }

    String typeName(bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;

    ~DateType() override = default;
};

class DateTimeType : public SQLType
{
public:
    const bool extended;
    const std::optional<const uint32_t> precision;
    const std::optional<const String> timezone;

    DateTimeType(const bool ex, const std::optional<const uint32_t> p, const std::optional<const String> t)
        : extended(ex), precision(p), timezone(t)
    {
    }

    String typeName(bool escape) const override;
    String MySQLtypeName(RandomGenerator & rg, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;

    ~DateTimeType() override = default;
};

class DecimalType : public SQLType
{
public:
    const std::optional<DecimalN_DecimalPrecision> short_notation;
    const std::optional<const uint32_t> precision, scale;
    DecimalType(
        const std::optional<DecimalN_DecimalPrecision> sn, const std::optional<const uint32_t> p, const std::optional<const uint32_t> s)
        : short_notation(sn), precision(p), scale(s)
    {
    }

    String typeName(bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator & rg, bool escape) const override;
    String SQLitetypeName(RandomGenerator & rg, bool escape) const override;

    ~DecimalType() override = default;
};

class StringType : public SQLType
{
public:
    const std::optional<const uint32_t> precision;
    explicit StringType(const std::optional<const uint32_t> p) : precision(p) { }

    String typeName(bool) const override;
    String MySQLtypeName(RandomGenerator & rg, bool) const override;
    String PostgreSQLtypeName(RandomGenerator & rg, bool) const override;
    String SQLitetypeName(RandomGenerator & rg, bool) const override;

    ~StringType() override = default;
};

class UUIDType : public SQLType
{
public:
    String typeName(bool) const override;
    String MySQLtypeName(RandomGenerator & rg, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool escape) const override;
    String SQLitetypeName(RandomGenerator & rg, bool) const override;

    ~UUIDType() override = default;
};

class EnumValue
{
public:
    const String val;
    const int32_t number;

    EnumValue(const String v, const int32_t n) : val(v), number(n) { }
};

class EnumType : public SQLType
{
public:
    const uint32_t size;
    const std::vector<EnumValue> values;
    EnumType(const uint32_t s, const std::vector<EnumValue> v) : size(s), values(v) { }

    String typeName(bool escape) const override;
    String MySQLtypeName(RandomGenerator & rg, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator & rg, bool) const override;

    ~EnumType() override = default;
};

class IPv4Type : public SQLType
{
public:
    String typeName(bool) const override;
    String MySQLtypeName(RandomGenerator & rg, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator & rg, bool) const override;

    ~IPv4Type() override = default;
};

class IPv6Type : public SQLType
{
public:
    String typeName(bool) const override;
    String MySQLtypeName(RandomGenerator & rg, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator & rg, bool) const override;

    ~IPv6Type() override = default;
};

class DynamicType : public SQLType
{
public:
    const std::optional<const uint32_t> ntypes;
    explicit DynamicType(const std::optional<const uint32_t> n) : ntypes(n) { }

    String typeName(bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;

    ~DynamicType() override = default;
};

class JSubType
{
public:
    const String cname;
    SQLType * subtype;

    JSubType(const String & n, SQLType * s) : cname(n), subtype(s) { }
};

class JSONType : public SQLType
{
public:
    const String desc;
    const std::vector<JSubType> subcols;
    explicit JSONType(const String & s, const std::vector<JSubType> sc) : desc(s), subcols(sc) { }

    String typeName(bool escape) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;

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

    String typeName(bool escape) const override;
    String MySQLtypeName(RandomGenerator & rg, bool escape) const override;
    String PostgreSQLtypeName(RandomGenerator & rg, bool escape) const override;
    String SQLitetypeName(RandomGenerator & rg, bool escape) const override;

    ~Nullable() override { delete subtype; }
};

class LowCardinality : public SQLType
{
public:
    SQLType * subtype;
    explicit LowCardinality(SQLType * s) : subtype(s) { }

    String typeName(bool escape) const override;
    String MySQLtypeName(RandomGenerator & rg, bool escape) const override;
    String PostgreSQLtypeName(RandomGenerator & rg, bool escape) const override;
    String SQLitetypeName(RandomGenerator & rg, bool escape) const override;

    ~LowCardinality() override { delete subtype; }
};

class GeoType : public SQLType
{
public:
    const GeoTypes geo_type;
    explicit GeoType(const GeoTypes & gt) : geo_type(gt) { }

    String typeName(bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;

    ~GeoType() override = default;
};

class ArrayType : public SQLType
{
public:
    SQLType * subtype;
    explicit ArrayType(SQLType * s) : subtype(s) { }

    String typeName(bool escape) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator & rg, bool escape) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;

    ~ArrayType() override { delete subtype; }
};

class MapType : public SQLType
{
public:
    SQLType *key, *value;
    MapType(SQLType * k, SQLType * v) : key(k), value(v) { }

    String typeName(bool escape) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;

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

    String typeName(bool escape) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;

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

    String typeName(bool escape) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;

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

    NestedSubType(const uint32_t n, SQLType * s) : cname(n), subtype(s), array_subtype(new ArrayType(typeDeepCopy(s))) { }
};

class NestedType : public SQLType
{
public:
    std::vector<NestedSubType> subtypes;
    explicit NestedType(std::vector<NestedSubType> s) : subtypes(s) { }

    String typeName(bool escape) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;

    ~NestedType() override
    {
        for (const auto & entry : subtypes)
        {
            delete entry.array_subtype;
            delete entry.subtype;
        }
    }
};

template <typename T>
bool hasType(const bool inside_array, bool inside_nullable, bool inside_nested, SQLType * tp)
{
    LowCardinality * lc;

    if (dynamic_cast<const T *>(tp))
    {
        return true;
    }
    if (inside_nullable)
    {
        Nullable * nl;

        if ((nl = dynamic_cast<Nullable *>(tp)))
        {
            return hasType<T>(inside_array, inside_nullable, inside_nested, nl->subtype);
        }
    }
    if ((lc = dynamic_cast<LowCardinality *>(tp)))
    {
        return hasType<T>(inside_array, inside_nullable, inside_nested, lc->subtype);
    }
    if (inside_array)
    {
        ArrayType * at;

        if ((at = dynamic_cast<ArrayType *>(tp)))
        {
            return hasType<T>(inside_array, inside_nullable, inside_nested, at->subtype);
        }
    }
    if (inside_nested)
    {
        TupleType * ttp;
        NestedType * ntp;

        if ((ttp = dynamic_cast<TupleType *>(tp)))
        {
            for (const auto & entry : ttp->subtypes)
            {
                if (hasType<T>(inside_array, inside_nullable, inside_nested, entry.subtype))
                {
                    return true;
                }
            }
        }
        else if ((ntp = dynamic_cast<NestedType *>(tp)))
        {
            for (const auto & entry : ntp->subtypes)
            {
                if (hasType<T>(inside_array, inside_nullable, inside_nested, entry.subtype))
                {
                    return true;
                }
            }
        }
    }
    return false;
}

String appendDecimal(RandomGenerator & rg, uint32_t left, uint32_t right);
String strBuildJSONArray(RandomGenerator & rg, int jdepth, int jwidth);
String strBuildJSONElement(RandomGenerator & rg);
String strBuildJSON(RandomGenerator & rg, int jdepth, int jwidth);
String strAppendGeoValue(RandomGenerator & rg, const GeoTypes & geo_type);

}
