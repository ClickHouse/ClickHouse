#pragma once

#include <base/types.h>

#include <concepts>
#include <map>
#include <memory>
#include <string_view>
#include <typeinfo>
#include <utility>
#include <variant>
#include <vector>

namespace DB::Iceberg
{

class JSONObject;
class JSONArray;

using JSONObjectPtr = std::shared_ptr<JSONObject>;
using JSONArrayPtr = std::shared_ptr<JSONArray>;

[[noreturn]] void throwJSONValueOutOfRange(std::string_view type_name);

class JSONValue
{
public:
    using Variant = std::variant<std::monostate, bool, Int64, UInt64, Float64, String, JSONObjectPtr, JSONArrayPtr>;

    JSONValue() = default;
    JSONValue(std::nullptr_t) {} /// NOLINT
    JSONValue(bool value_) : value(value_) {} /// NOLINT

    template <std::integral T>
    requires (!std::same_as<T, bool>)
    JSONValue(T value_) /// NOLINT
    {
        if constexpr (std::is_signed_v<T>)
            value = static_cast<Int64>(value_);
        else if (std::in_range<Int64>(value_))
            value = static_cast<Int64>(value_);
        else
            value = static_cast<UInt64>(value_);
    }

    template <std::floating_point T>
    JSONValue(T value_) : value(static_cast<Float64>(value_)) {} /// NOLINT

    JSONValue(String value_) : value(std::move(value_)) {} /// NOLINT
    JSONValue(const char * value_) : value(String(value_)) {} /// NOLINT
    JSONValue(std::string_view value_) : value(String(value_)) {} /// NOLINT
    JSONValue(JSONObjectPtr value_); /// NOLINT
    JSONValue(JSONArrayPtr value_); /// NOLINT

    bool isNull() const { return std::holds_alternative<std::monostate>(value); }
    bool isBool() const { return std::holds_alternative<bool>(value); }
    bool isInteger() const { return std::holds_alternative<Int64>(value) || std::holds_alternative<UInt64>(value); }
    bool isNumber() const { return isInteger() || std::holds_alternative<Float64>(value); }
    bool isString() const { return std::holds_alternative<String>(value); }
    bool isObject() const { return std::holds_alternative<JSONObjectPtr>(value); }
    bool isArray() const { return std::holds_alternative<JSONArrayPtr>(value); }

    const String & getString() const;
    const JSONObjectPtr & getObject() const;
    const JSONArrayPtr & getArray() const;

    template <typename T>
    T getValue() const
    {
        if constexpr (std::is_same_v<T, bool>)
        {
            return toBool();
        }
        else if constexpr (std::is_integral_v<T> && std::is_signed_v<T>)
        {
            Int64 result = toInt64();
            if (!std::in_range<T>(result))
                throwJSONValueOutOfRange(typeid(T).name());
            return static_cast<T>(result);
        }
        else if constexpr (std::is_integral_v<T>)
        {
            UInt64 result = toUInt64();
            if (!std::in_range<T>(result))
                throwJSONValueOutOfRange(typeid(T).name());
            return static_cast<T>(result);
        }
        else if constexpr (std::is_floating_point_v<T>)
        {
            return static_cast<T>(toFloat64());
        }
        else
        {
            static_assert(std::is_same_v<T, String>);
            return toString();
        }
    }

    String toString() const;

    const Variant & getVariant() const { return value; }

    JSONValue clone() const;

    bool operator==(const JSONValue & other) const;

private:
    bool toBool() const;
    Int64 toInt64() const;
    UInt64 toUInt64() const;
    Float64 toFloat64() const;

    Variant value;
};

class JSONObject
{
public:
    using Ptr = JSONObjectPtr;
    using Values = std::map<String, JSONValue, std::less<>>;
    using ConstIterator = Values::const_iterator;

    static Ptr create() { return std::make_shared<JSONObject>(); }

    bool has(std::string_view key) const { return values.contains(key); }
    bool isNull(std::string_view key) const;
    bool isObject(std::string_view key) const { return get(key).isObject(); }
    bool isArray(std::string_view key) const { return get(key).isArray(); }

    const JSONValue & get(std::string_view key) const;

    template <typename T>
    T getValue(std::string_view key) const
    {
        return getExisting(key).getValue<T>();
    }

    JSONObjectPtr getObject(std::string_view key) const;
    JSONArrayPtr getArray(std::string_view key) const;

    void set(std::string_view key, JSONValue value);
    void remove(std::string_view key);
    void clear() { values.clear(); }

    size_t size() const { return values.size(); }
    bool empty() const { return values.empty(); }
    std::vector<String> getNames() const;

    ConstIterator begin() const { return values.begin(); }
    ConstIterator end() const { return values.end(); }

    Ptr clone() const;

    bool operator==(const JSONObject & other) const { return values == other.values; }

private:
    const JSONValue & getExisting(std::string_view key) const;

    Values values;
};

class JSONArray
{
public:
    using Ptr = JSONArrayPtr;
    using Values = std::vector<JSONValue>;
    using ConstIterator = Values::const_iterator;

    static Ptr create() { return std::make_shared<JSONArray>(); }

    const JSONValue & get(size_t index) const;

    template <typename T>
    T getElement(size_t index) const
    {
        return getExisting(index).getValue<T>();
    }

    JSONObjectPtr getObject(size_t index) const;
    JSONArrayPtr getArray(size_t index) const;

    void add(JSONValue value) { values.push_back(std::move(value)); }
    void set(size_t index, JSONValue value);
    void remove(size_t index);
    void clear() { values.clear(); }
    void reserve(size_t size) { values.reserve(size); }

    size_t size() const { return values.size(); }
    bool empty() const { return values.empty(); }

    ConstIterator begin() const { return values.begin(); }
    ConstIterator end() const { return values.end(); }

    Ptr clone() const;

    bool operator==(const JSONArray & other) const { return values == other.values; }

private:
    const JSONValue & getExisting(size_t index) const;

    Values values;
};

JSONValue parseJSON(std::string_view json);
JSONObjectPtr parseJSONObject(std::string_view json);

String stringifyJSON(const JSONValue & json, unsigned indent = 0);

}
