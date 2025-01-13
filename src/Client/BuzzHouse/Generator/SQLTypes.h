#pragma once

#include <Client/BuzzHouse/AST/SQLProtoStr.h>
#include <Client/BuzzHouse/Generator/RandomGenerator.h>

#include <memory>
#include <optional>

namespace BuzzHouse
{

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

    void typeName(std::string & ret, const bool) const override;
    void MySQLtypeName(RandomGenerator &, std::string & ret, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override;
    void SQLitetypeName(RandomGenerator &, std::string & ret, const bool) const override;

    ~IntType() override = default;
};

class FloatType : public SQLType
{
public:
    const uint32_t size;
    explicit FloatType(const uint32_t s) : size(s) { }

    void typeName(std::string & ret, const bool) const override;
    void MySQLtypeName(RandomGenerator &, std::string & ret, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override;
    void SQLitetypeName(RandomGenerator &, std::string & ret, const bool) const override;

    ~FloatType() override = default;
};

class DateType : public SQLType
{
public:
    const bool extended;
    explicit DateType(const bool ex) : extended(ex) { }

    void typeName(std::string & ret, const bool) const override;
    void MySQLtypeName(RandomGenerator &, std::string & ret, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override;
    void SQLitetypeName(RandomGenerator &, std::string & ret, const bool) const override;

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

    void typeName(std::string & ret, const bool escape) const override;
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override;
    void SQLitetypeName(RandomGenerator &, std::string & ret, const bool) const override;

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

    void typeName(std::string & ret, const bool) const override;
    void MySQLtypeName(RandomGenerator &, std::string & ret, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override;
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override;

    ~DecimalType() override = default;
};

class StringType : public SQLType
{
public:
    const std::optional<const uint32_t> precision;
    explicit StringType(const std::optional<const uint32_t> p) : precision(p) { }

    void typeName(std::string & ret, const bool) const override;
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator & rg, std::string & ret, const bool) const override;
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool) const override;

    ~StringType() override = default;
};

class UUIDType : public SQLType
{
public:
    void typeName(std::string & ret, const bool) const override;
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool escape) const override;
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool) const override;

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

    void typeName(std::string & ret, const bool escape) const override;
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override;
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool) const override;

    ~EnumType() override = default;
};

class IPv4Type : public SQLType
{
public:
    void typeName(std::string & ret, const bool) const override;
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override;
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool) const override;

    ~IPv4Type() override = default;
};

class IPv6Type : public SQLType
{
public:
    void typeName(std::string & ret, const bool) const override;
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override;
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool) const override;

    ~IPv6Type() override = default;
};

class DynamicType : public SQLType
{
public:
    const std::optional<const uint32_t> ntypes;
    explicit DynamicType(const std::optional<const uint32_t> n) : ntypes(n) { }

    void typeName(std::string & ret, const bool) const override;
    void MySQLtypeName(RandomGenerator &, std::string &, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string &, const bool) const override;
    void SQLitetypeName(RandomGenerator &, std::string &, const bool) const override;

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

    void typeName(std::string & ret, const bool escape) const override;
    void MySQLtypeName(RandomGenerator &, std::string & ret, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string & ret, const bool) const override;
    void SQLitetypeName(RandomGenerator &, std::string & ret, const bool) const override;

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

    void typeName(std::string & ret, const bool escape) const override;
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override;
    void PostgreSQLtypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override;
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override;

    ~Nullable() override { delete subtype; }
};

class LowCardinality : public SQLType
{
public:
    SQLType * subtype;
    explicit LowCardinality(SQLType * s) : subtype(s) { }

    void typeName(std::string & ret, const bool escape) const override;
    void MySQLtypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override;
    void PostgreSQLtypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override;
    void SQLitetypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override;

    ~LowCardinality() override { delete subtype; }
};

class GeoType : public SQLType
{
public:
    const GeoTypes geo_type;
    explicit GeoType(const GeoTypes & gt) : geo_type(gt) { }

    void typeName(std::string & ret, const bool) const override;
    void MySQLtypeName(RandomGenerator &, std::string &, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string &, const bool) const override;
    void SQLitetypeName(RandomGenerator &, std::string &, const bool) const override;

    ~GeoType() override = default;
};

class ArrayType : public SQLType
{
public:
    SQLType * subtype;
    explicit ArrayType(SQLType * s) : subtype(s) { }

    void typeName(std::string & ret, const bool escape) const override;
    void MySQLtypeName(RandomGenerator &, std::string &, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override;
    void SQLitetypeName(RandomGenerator &, std::string &, const bool) const override;

    ~ArrayType() override { delete subtype; }
};

class MapType : public SQLType
{
public:
    SQLType *key, *value;
    MapType(SQLType * k, SQLType * v) : key(k), value(v) { }

    void typeName(std::string & ret, const bool escape) const override;
    void MySQLtypeName(RandomGenerator &, std::string &, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string &, const bool) const override;
    void SQLitetypeName(RandomGenerator &, std::string &, const bool) const override;

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

    void typeName(std::string & ret, const bool escape) const override;
    void MySQLtypeName(RandomGenerator &, std::string &, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string &, const bool) const override;
    void SQLitetypeName(RandomGenerator &, std::string &, const bool) const override;

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

    void typeName(std::string & ret, const bool escape) const override;
    void MySQLtypeName(RandomGenerator &, std::string &, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string &, const bool) const override;
    void SQLitetypeName(RandomGenerator &, std::string &, const bool) const override;

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

    void typeName(std::string & ret, const bool escape) const override;
    void MySQLtypeName(RandomGenerator &, std::string &, const bool) const override;
    void PostgreSQLtypeName(RandomGenerator &, std::string &, const bool) const override;
    void SQLitetypeName(RandomGenerator &, std::string &, const bool) const override;

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
