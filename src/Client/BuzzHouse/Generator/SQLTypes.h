#pragma once

#include <Client/BuzzHouse/AST/SQLProtoStr.h>
#include <Client/BuzzHouse/Generator/RandomGenerator.h>

#include <memory>
#include <optional>

namespace BuzzHouse
{

class StatementGenerator;

enum class SQLTypeClass
{
    NONE = 0,
    BOOL = 1,
    INT = 2,
    FLOAT = 3,
    DATE = 4,
    TIME = 5,
    DATETIME = 6,
    DECIMAL = 7,
    STRING = 8,
    UUID = 9,
    ENUM = 10,
    IPV4 = 11,
    IPV6 = 12,
    DYNAMIC = 13,
    JSON = 14,
    NULLABLE = 15,
    LOWCARDINALITY = 16,
    GEO = 17,
    ARRAY = 18,
    MAP = 19,
    TUPLE = 20,
    VARIANT = 21,
    QBIT = 22,
    AGGREGATEFUNCTION = 23,
    NESTED = 24
};

class SQLType
{
public:
    virtual String typeName(bool, bool) const = 0;
    virtual String MySQLtypeName(RandomGenerator & rg, bool escape) const = 0;
    virtual String PostgreSQLtypeName(RandomGenerator & rg, bool escape) const = 0;
    virtual String SQLitetypeName(RandomGenerator & rg, bool escape) const = 0;
    virtual std::unique_ptr<SQLType> typeDeepCopy() const = 0;
    virtual String appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen) const = 0;
    virtual String insertNumberEntry(RandomGenerator & rg, StatementGenerator & gen, uint32_t max_strlen, uint32_t max_nested_rows) const
        = 0;
    virtual bool isNullable() const = 0;
    virtual SQLTypeClass getTypeClass() const = 0;

    virtual ~SQLType() = default;
};

class BoolType : public SQLType
{
public:
    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return true; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::BOOL; }

    ~BoolType() override = default;
};

class IntType : public SQLType
{
public:
    const uint32_t size;
    const bool is_unsigned;
    IntType(const uint32_t s, bool isu)
        : size(s)
        , is_unsigned(isu)
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return true; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::INT; }

    ~IntType() override = default;
};

class FloatType : public SQLType
{
public:
    const uint32_t size;
    explicit FloatType(const uint32_t s)
        : size(s)
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return true; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::FLOAT; }

    ~FloatType() override = default;
};

class DateType : public SQLType
{
public:
    const bool extended;
    explicit DateType(const bool ex)
        : extended(ex)
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return true; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::DATE; }

    ~DateType() override = default;
};

class TimeType : public SQLType
{
public:
    const bool extended;
    const std::optional<const uint32_t> precision;

    TimeType(const bool ex, const std::optional<const uint32_t> p)
        : extended(ex)
        , precision(p)
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return true; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::TIME; }

    ~TimeType() override = default;
};

class DateTimeType : public SQLType
{
public:
    const bool extended;
    const std::optional<const uint32_t> precision;
    const std::optional<const String> timezone;

    DateTimeType(const bool ex, const std::optional<const uint32_t> p, const std::optional<const String> t)
        : extended(ex)
        , precision(p)
        , timezone(t)
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator & rg, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return true; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::DATETIME; }

    ~DateTimeType() override = default;
};

class DecimalType : public SQLType
{
public:
    const std::optional<DecimalN_DecimalPrecision> short_notation;
    const std::optional<const uint32_t> precision, scale;
    DecimalType(
        const std::optional<DecimalN_DecimalPrecision> sn, const std::optional<const uint32_t> p, const std::optional<const uint32_t> s)
        : short_notation(sn)
        , precision(p)
        , scale(s)
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator & rg, bool escape) const override;
    String SQLitetypeName(RandomGenerator & rg, bool escape) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    static String appendDecimalValue(RandomGenerator & rg, bool use_func, const DecimalType * dt);
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return true; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::DECIMAL; }

    ~DecimalType() override = default;
};

class StringType : public SQLType
{
public:
    const std::optional<const uint32_t> precision;
    explicit StringType(const std::optional<const uint32_t> p)
        : precision(p)
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator & rg, bool) const override;
    String PostgreSQLtypeName(RandomGenerator & rg, bool) const override;
    String SQLitetypeName(RandomGenerator & rg, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return true; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::STRING; }

    ~StringType() override = default;
};

class UUIDType : public SQLType
{
public:
    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator & rg, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool escape) const override;
    String SQLitetypeName(RandomGenerator & rg, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return true; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::UUID; }

    ~UUIDType() override = default;
};

class EnumValue
{
public:
    const String val;
    const int32_t number;

    EnumValue(const String v, const int32_t n)
        : val(v)
        , number(n)
    {
    }
};

class EnumType : public SQLType
{
public:
    const uint32_t size;
    const std::vector<EnumValue> values;
    EnumType(const uint32_t s, const std::vector<EnumValue> v)
        : size(s)
        , values(v)
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator & rg, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator & rg, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return true; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::ENUM; }

    ~EnumType() override = default;
};

class IPv4Type : public SQLType
{
public:
    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator & rg, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator & rg, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return true; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::IPV4; }

    ~IPv4Type() override = default;
};

class IPv6Type : public SQLType
{
public:
    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator & rg, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator & rg, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return true; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::IPV6; }

    ~IPv6Type() override = default;
};

class DynamicType : public SQLType
{
public:
    const std::optional<const uint32_t> ntypes;
    explicit DynamicType(const std::optional<const uint32_t> n)
        : ntypes(n)
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return false; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::DYNAMIC; }

    ~DynamicType() override = default;
};

class JSubType
{
public:
    const String cname;
    std::unique_ptr<SQLType> subtype;

    JSubType(const String & n, std::unique_ptr<SQLType> s)
        : cname(n)
        , subtype(std::move(s))
    {
    }
};

class JSONType : public SQLType
{
public:
    const String desc;
    std::vector<JSubType> subcols;
    explicit JSONType(const String & s, std::vector<JSubType> sc)
        : desc(s)
        , subcols(std::move(sc))
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return false; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::JSON; }

    ~JSONType() override = default;
};

class Nullable : public SQLType
{
public:
    std::unique_ptr<SQLType> subtype;
    explicit Nullable(std::unique_ptr<SQLType> s)
        : subtype(std::move(s))
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator & rg, bool escape) const override;
    String PostgreSQLtypeName(RandomGenerator & rg, bool escape) const override;
    String SQLitetypeName(RandomGenerator & rg, bool escape) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return false; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::NULLABLE; }

    ~Nullable() override = default;
};

class LowCardinality : public SQLType
{
public:
    std::unique_ptr<SQLType> subtype;
    explicit LowCardinality(std::unique_ptr<SQLType> s)
        : subtype(std::move(s))
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator & rg, bool escape) const override;
    String PostgreSQLtypeName(RandomGenerator & rg, bool escape) const override;
    String SQLitetypeName(RandomGenerator & rg, bool escape) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return false; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::LOWCARDINALITY; }

    ~LowCardinality() override = default;
};

class GeoType : public SQLType
{
public:
    const GeoTypes geotype;
    explicit GeoType(const GeoTypes & gt)
        : geotype(gt)
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return false; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::GEO; }

    ~GeoType() override = default;
};

class ArrayType : public SQLType
{
public:
    std::unique_ptr<SQLType> subtype;
    explicit ArrayType(std::unique_ptr<SQLType> s)
        : subtype(std::move(s))
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator & rg, bool escape) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    static String appendRandomRawValue(RandomGenerator & rg, StatementGenerator & gen, const SQLType * tp, uint64_t limit);
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return false; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::ARRAY; }

    ~ArrayType() override = default;
};

class MapType : public SQLType
{
public:
    std::unique_ptr<SQLType> key;
    std::unique_ptr<SQLType> value;
    MapType(std::unique_ptr<SQLType> k, std::unique_ptr<SQLType> v)
        : key(std::move(k))
        , value(std::move(v))
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return false; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::MAP; }

    ~MapType() override = default;
};

class SubType
{
public:
    const std::optional<const uint32_t> cname;
    std::unique_ptr<SQLType> subtype;

    SubType(const std::optional<const uint32_t> n, std::unique_ptr<SQLType> s)
        : cname(n)
        , subtype(std::move(s))
    {
    }
};

class TupleType : public SQLType
{
public:
    const bool nullable;
    std::vector<SubType> subtypes;
    explicit TupleType(const bool n, std::vector<SubType> s)
        : nullable(n)
        , subtypes(std::move(s))
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return !nullable; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::TUPLE; }

    ~TupleType() override = default;
};

class VariantType : public SQLType
{
public:
    std::vector<std::unique_ptr<SQLType>> subtypes;
    explicit VariantType(std::vector<std::unique_ptr<SQLType>> s)
        : subtypes(std::move(s))
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return false; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::VARIANT; }

    ~VariantType() override = default;
};

class QBitType : public SQLType
{
public:
    std::unique_ptr<SQLType> subtype;
    const uint32_t dimension;

    QBitType(std::unique_ptr<SQLType> s, const uint32_t d)
        : subtype(std::move(s))
        , dimension(d)
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return true; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::QBIT; }

    ~QBitType() override = default;
};

class AggregateFunctionType : public SQLType
{
public:
    const bool simple;
    const SQLFunc aggregate;
    std::vector<std::unique_ptr<SQLType>> subtypes;

    AggregateFunctionType(const bool s, const SQLFunc aggr, std::vector<std::unique_ptr<SQLType>> subs)
        : simple(s)
        , aggregate(aggr)
        , subtypes(std::move(subs))
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return simple; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::AGGREGATEFUNCTION; }

    ~AggregateFunctionType() override = default;
};

class NestedSubType
{
public:
    uint32_t cname;
    std::unique_ptr<SQLType> subtype;

    NestedSubType(const uint32_t n, std::unique_ptr<SQLType> s)
        : cname(n)
        , subtype(std::move(s))
    {
    }
};

class NestedType : public SQLType
{
public:
    std::vector<NestedSubType> subtypes;
    explicit NestedType(std::vector<NestedSubType> s)
        : subtypes(std::move(s))
    {
    }

    String typeName(bool, bool) const override;
    String MySQLtypeName(RandomGenerator &, bool) const override;
    String PostgreSQLtypeName(RandomGenerator &, bool) const override;
    String SQLitetypeName(RandomGenerator &, bool) const override;
    std::unique_ptr<SQLType> typeDeepCopy() const override;
    String appendRandomRawValue(RandomGenerator &, StatementGenerator &) const override;
    String insertNumberEntry(RandomGenerator &, StatementGenerator &, uint32_t, uint32_t) const override;
    bool isNullable() const override { return false; }
    SQLTypeClass getTypeClass() const override { return SQLTypeClass::NESTED; }

    ~NestedType() override = default;
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
            return hasType<T>(inside_array, inside_nullable, inside_nested, nl->subtype.get());
        }
    }
    if ((lc = dynamic_cast<LowCardinality *>(tp)))
    {
        return hasType<T>(inside_array, inside_nullable, inside_nested, lc->subtype.get());
    }
    if (inside_array)
    {
        ArrayType * at;

        if ((at = dynamic_cast<ArrayType *>(tp)))
        {
            return hasType<T>(inside_array, inside_nullable, inside_nested, at->subtype.get());
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
                if (hasType<T>(inside_array, inside_nullable, inside_nested, entry.subtype.get()))
                {
                    return true;
                }
            }
        }
        else if ((ntp = dynamic_cast<NestedType *>(tp)))
        {
            for (const auto & entry : ntp->subtypes)
            {
                if (hasType<T>(inside_array, inside_nullable, inside_nested, entry.subtype.get()))
                {
                    return true;
                }
            }
        }
    }
    return false;
}

String appendDecimal(RandomGenerator & rg, bool use_func, uint32_t left, uint32_t right);
String strBuildJSONArray(RandomGenerator & rg, int jdepth, int jwidth);
String strBuildJSONElement(RandomGenerator & rg);
String strBuildJSON(RandomGenerator & rg, int jdepth, int jwidth);
String strAppendGeoValue(RandomGenerator & rg, const GeoTypes & gt);

}
