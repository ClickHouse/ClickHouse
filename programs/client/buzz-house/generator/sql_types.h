#pragma once

#include "buzz-house/ast/sql_grammar.pb.h"
#include "random_generator.h"

#include <memory>
#include <optional>

namespace buzzhouse
{

const constexpr uint32_t allow_bool = (1 << 0), allow_unsigned_int = (1 << 1), allow_int8 = (1 << 2), allow_hugeint = (1 << 3),
                         allow_floating_points = (1 << 4), allow_dates = (1 << 5), allow_date32 = (1 << 6), allow_datetime64 = (1 << 7),
                         allow_strings = (1 << 8), allow_decimals = (1 << 9), allow_uuid = (1 << 10), allow_enum = (1 << 11),
                         allow_dynamic = (1 << 12), allow_json = (1 << 13), allow_nullable = (1 << 14), allow_low_cardinality = (1 << 15),
                         allow_array = (1 << 16), allow_map = (1 << 17), allow_tuple = (1 << 18), allow_variant = (1 << 19),
                         allow_nested = (1 << 20), allow_nullable_inside_array = (1 << 21);

class SQLType
{
public:
    virtual void TypeName(std::string & ret, const bool escape) const = 0;
    virtual void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const = 0;
    virtual void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const = 0;
    virtual void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const = 0;

    virtual ~SQLType() = default;
};

const SQLType * TypeDeepCopy(const SQLType * tp);

class BoolType : public SQLType
{
public:
    void TypeName(std::string & ret, const bool escape) const override
    {
        (void)escape;
        ret += "Bool";
    }
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)escape;
        ret += "BOOL";
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)escape;
        ret += "BOOLEAN";
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)escape;
        ret += "INTEGER";
    }

    ~BoolType() override = default;
};

class IntType : public SQLType
{
public:
    const uint32_t size;
    const bool is_unsigned;
    IntType(const uint32_t s, const bool isu) : size(s), is_unsigned(isu) { }

    void TypeName(std::string & ret, const bool escape) const override
    {
        (void)escape;
        ret += is_unsigned ? "U" : "";
        ret += "Int";
        ret += std::to_string(size);
    }
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)escape;
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
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)escape;
        switch (size)
        {
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
        assert(!is_unsigned);
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)escape;
        ret += "INTEGER";
    }

    ~IntType() override = default;
};

class FloatType : public SQLType
{
public:
    const uint32_t size;
    FloatType(const uint32_t s) : size(s) { }

    void TypeName(std::string & ret, const bool escape) const override
    {
        (void)escape;
        ret += "Float";
        ret += std::to_string(size);
    }
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)escape;
        ret += (size == 32) ? "FLOAT" : "DOUBLE";
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)escape;
        ret += (size == 32) ? "REAL" : "DOUBLE PRECISION";
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }

    ~FloatType() override = default;
};

class DateType : public SQLType
{
public:
    const bool has_time, extended;
    DateType(const bool ht, const bool ex) : has_time(ht), extended(ex) { }

    void TypeName(std::string & ret, const bool escape) const override
    {
        (void)escape;
        ret += "Date";
        ret += has_time ? "Time" : "";
        if (extended)
        {
            ret += has_time ? "64" : "32";
        }
    }
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)escape;
        ret += has_time ? (rg.NextBool() ? "DATETIME" : "TIMESTAMP") : "DATE";
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)escape;
        ret += has_time ? "TIMESTAMP" : "DATE";
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }

    ~DateType() override = default;
};

class DecimalType : public SQLType
{
public:
    const std::optional<const uint32_t> precision, scale;
    DecimalType(const std::optional<const uint32_t> p, const std::optional<const uint32_t> s) : precision(p), scale(s) { }

    void TypeName(std::string & ret, const bool escape) const override
    {
        (void)escape;
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
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        std::string next;

        (void)rg;
        TypeName(next, escape);
        std::transform(next.begin(), next.end(), next.begin(), [](unsigned char c) { return std::toupper(c); });
        ret += next;
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override { MySQLTypeName(rg, ret, escape); }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override { MySQLTypeName(rg, ret, escape); }

    ~DecimalType() override = default;
};

class StringType : public SQLType
{
public:
    const std::optional<const uint32_t> precision;
    StringType(const std::optional<const uint32_t> p) : precision(p) { }

    void TypeName(std::string & ret, const bool escape) const override
    {
        (void)escape;
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
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)escape;
        if (precision.has_value())
        {
            ret += rg.NextBool() ? "VAR" : "";
            ret += rg.NextBool() ? "CHAR" : "BINARY";
            ret += "(";
            ret += std::to_string(precision.value());
            ret += ")";
        }
        else
        {
            ret += rg.NextBool() ? "BLOB" : "TEXT";
        }
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)escape;
        if (precision.has_value())
        {
            ret += rg.NextBool() ? "VAR" : "";
            ret += "CHAR(";
            ret += std::to_string(precision.value());
            ret += ")";
        }
        else
        {
            ret += "TEXT";
        }
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)escape;
        ret += rg.NextBool() ? "BLOB" : "TEXT";
    }

    ~StringType() override = default;
};

class UUIDType : public SQLType
{
public:
    void TypeName(std::string & ret, const bool escape) const override
    {
        (void)escape;
        ret += "UUID";
    }
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        TypeName(ret, escape);
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        TypeName(ret, escape);
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)escape;
        ret += rg.NextBool() ? "BLOB" : "TEXT";
    }

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
    const std::vector<const EnumValue> values;
    EnumType(const uint32_t s, const std::vector<const EnumValue> v) : size(s), values(v) { }

    void TypeName(std::string & ret, const bool escape) const override
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
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }

    ~EnumType() override = default;
};

class DynamicType : public SQLType
{
public:
    const std::optional<const uint32_t> ntypes;
    DynamicType(const std::optional<const uint32_t> n) : ntypes(n) { }

    void TypeName(std::string & ret, const bool escape) const override
    {
        (void)escape;
        ret += "Dynamic";
        if (ntypes.has_value())
        {
            ret += "(max_types=";
            ret += std::to_string(ntypes.value());
            ret += ")";
        }
    }
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }

    ~DynamicType() override = default;
};

class JSONType : public SQLType
{
public:
    const std::string desc;
    JSONType(const std::string & s) : desc(s) { }

    void TypeName(std::string & ret, const bool escape) const override
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
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)escape;
        ret += "JSON";
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)escape;
        ret += "JSON";
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        ret += "TEXT";
    }

    ~JSONType() override = default;
};

class Nullable : public SQLType
{
public:
    const SQLType * subtype;
    Nullable(const SQLType * s) : subtype(s) { }

    void TypeName(std::string & ret, const bool escape) const override
    {
        ret += "Nullable(";
        subtype->TypeName(ret, escape);
        ret += ")";
    }
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        subtype->MySQLTypeName(rg, ret, escape);
        ret += " NULL";
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        subtype->PostgreSQLTypeName(rg, ret, escape);
        ret += " NULL";
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        subtype->SQLiteTypeName(rg, ret, escape);
        ret += " NULL";
    }

    ~Nullable() override { delete subtype; }
};

class LowCardinality : public SQLType
{
public:
    const SQLType * subtype;
    LowCardinality(const SQLType * s) : subtype(s) { }

    void TypeName(std::string & ret, const bool escape) const override
    {
        ret += "LowCardinality(";
        subtype->TypeName(ret, escape);
        ret += ")";
    }
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        subtype->MySQLTypeName(rg, ret, escape);
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        subtype->PostgreSQLTypeName(rg, ret, escape);
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        subtype->SQLiteTypeName(rg, ret, escape);
    }

    ~LowCardinality() override { delete subtype; }
};

class ArrayType : public SQLType
{
public:
    const SQLType * subtype;
    ArrayType(const SQLType * s) : subtype(s) { }

    void TypeName(std::string & ret, const bool escape) const override
    {
        ret += "Array(";
        subtype->TypeName(ret, escape);
        ret += ")";
    }
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        subtype->PostgreSQLTypeName(rg, ret, escape);
        ret += "[]";
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }

    ~ArrayType() override { delete subtype; }
};

class MapType : public SQLType
{
public:
    const SQLType *key, *value;
    MapType(const SQLType * k, const SQLType * v) : key(k), value(v) { }

    void TypeName(std::string & ret, const bool escape) const override
    {
        ret += "Map(";
        key->TypeName(ret, escape);
        ret += ",";
        value->TypeName(ret, escape);
        ret += ")";
    }
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }

    ~MapType() override
    {
        delete key;
        delete value;
    }
};

class SubType
{
public:
    const uint32_t cname;
    const SQLType * subtype;

    SubType(const uint32_t n, const SQLType * s) : cname(n), subtype(s) { }
};

class TupleType : public SQLType
{
public:
    const std::vector<const SubType> subtypes;
    TupleType(const std::vector<const SubType> s) : subtypes(s) { }

    void TypeName(std::string & ret, const bool escape) const override
    {
        ret += "Tuple(";
        for (uint32_t i = 0; i < subtypes.size(); i++)
        {
            if (i != 0)
            {
                ret += ",";
            }
            ret += "c";
            ret += std::to_string(subtypes[i].cname);
            ret += " ";
            subtypes[i].subtype->TypeName(ret, escape);
        }
        ret += ")";
    }
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }

    ~TupleType() override
    {
        for (auto & entry : subtypes)
        {
            delete entry.subtype;
        }
    }
};

class VariantType : public SQLType
{
public:
    const std::vector<const SQLType *> subtypes;
    VariantType(const std::vector<const SQLType *> s) : subtypes(s) { }

    void TypeName(std::string & ret, const bool escape) const override
    {
        ret += "Variant(";
        for (uint32_t i = 0; i < subtypes.size(); i++)
        {
            if (i != 0)
            {
                ret += ",";
            }
            subtypes[i]->TypeName(ret, escape);
        }
        ret += ")";
    }
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }

    ~VariantType() override
    {
        for (auto & entry : subtypes)
        {
            delete entry;
        }
    }
};

class NestedSubType
{
public:
    const uint32_t cname;
    const SQLType * subtype;
    const ArrayType * array_subtype;

    NestedSubType(const uint32_t n, const SQLType * s) : cname(n), subtype(s), array_subtype(new ArrayType(TypeDeepCopy(s))) { }
};

class NestedType : public SQLType
{
public:
    const std::vector<const NestedSubType> subtypes;
    NestedType(const std::vector<const NestedSubType> s) : subtypes(s) { }

    void TypeName(std::string & ret, const bool escape) const override
    {
        ret += "Nested(";
        for (uint32_t i = 0; i < subtypes.size(); i++)
        {
            if (i != 0)
            {
                ret += ",";
            }
            ret += "c";
            ret += std::to_string(subtypes[i].cname);
            ret += " ";
            subtypes[i].subtype->TypeName(ret, escape);
        }
        ret += ")";
    }
    void MySQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }
    void PostgreSQLTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }
    void SQLiteTypeName(RandomGenerator & rg, std::string & ret, const bool escape) const override
    {
        (void)rg;
        (void)ret;
        (void)escape;
        assert(0);
    }

    ~NestedType() override
    {
        for (auto & entry : subtypes)
        {
            delete entry.array_subtype;
            delete entry.subtype;
        }
    }
};

template <typename T>
bool HasType(const SQLType * tp)
{
    const Nullable * nl;
    const LowCardinality * lc;
    const ArrayType * at;

    if (dynamic_cast<const T *>(tp))
    {
        return true;
    }
    else if ((nl = dynamic_cast<const Nullable *>(tp)))
    {
        return HasType<T>(nl->subtype);
    }
    else if ((lc = dynamic_cast<const LowCardinality *>(tp)))
    {
        return HasType<T>(lc->subtype);
    }
    else if ((at = dynamic_cast<const ArrayType *>(tp)))
    {
        return HasType<T>(at->subtype);
    }
    return false;
}

std::tuple<const SQLType *, sql_query_grammar::Integers> RandomIntType(RandomGenerator & rg, const uint32_t allowed_types);
std::tuple<const SQLType *, sql_query_grammar::FloatingPoints> RandomFloatType(RandomGenerator & rg);
std::tuple<const SQLType *, sql_query_grammar::Dates> RandomDateType(RandomGenerator & rg, const uint32_t allowed_types);

}
