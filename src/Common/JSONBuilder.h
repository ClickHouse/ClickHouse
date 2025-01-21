#pragma once
#include <type_traits>
#include <vector>
#include <IO/WriteBuffer.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteHelpers.h>

namespace DB::JSONBuilder
{

struct FormatSettings
{
    const DB::FormatSettings & settings;
    size_t indent = 2;
    bool print_simple_arrays_in_single_row = true;
    bool solid = false; // the output will not contain spaces and line breaks
};

struct FormatContext
{
    WriteBuffer & out;
    size_t offset = 0;
};

class IItem
{
public:
    virtual ~IItem() = default;
    virtual void format(const FormatSettings & settings, FormatContext & context) = 0;
};

using ItemPtr = std::unique_ptr<IItem>;

class JSONString : public IItem
{
public:
    explicit JSONString(std::string_view value_) : value(value_) {}
    void format(const FormatSettings & settings, FormatContext & context) override;

private:
    std::string value;
};

template <typename T>
class JSONNumber : public IItem
{
public:
    explicit JSONNumber(T value_) : value(value_)
    {
        static_assert(std::is_arithmetic_v<T>, "JSONNumber support only numeric types");
    }

    void format(const FormatSettings & settings, FormatContext & context) override
    {
        writeJSONNumber(value, context.out, settings.settings);
    }

private:
    T value;
};

class JSONBool : public IItem
{
public:
    explicit JSONBool(bool value_) : value(value_) {}
    void format(const FormatSettings & settings, FormatContext & context) override;

private:
    bool value;
};

class JSONArray : public IItem
{
public:
    void add(ItemPtr value) { values.push_back(std::move(value)); }
    void add(std::string value) { add(std::make_unique<JSONString>(std::move(value))); }
    void add(const char * value) { add(std::make_unique<JSONString>(value)); }
    void add(bool value) { add(std::make_unique<JSONBool>(value)); }

    template <typename T>
    requires std::is_arithmetic_v<T>
    void add(T value) { add(std::make_unique<JSONNumber<T>>(value)); }

    void format(const FormatSettings & settings, FormatContext & context) override;

private:
    std::vector<ItemPtr> values;
};

class JSONMap : public IItem
{
    struct Pair
    {
        std::string key;
        ItemPtr value;
    };

public:
    void add(std::string key, ItemPtr value) { values.emplace_back(Pair{.key = std::move(key), .value = std::move(value)}); }
    void add(std::string key, std::string value) { add(std::move(key), std::make_unique<JSONString>(std::move(value))); }
    void add(std::string key, const char * value) { add(std::move(key), std::make_unique<JSONString>(value)); }
    void add(std::string key, std::string_view value) { add(std::move(key), std::make_unique<JSONString>(value)); }
    void add(std::string key, bool value) { add(std::move(key), std::make_unique<JSONBool>(value)); }

    template <typename T>
    requires std::is_arithmetic_v<T>
    void add(std::string key, T value) { add(std::move(key), std::make_unique<JSONNumber<T>>(value)); }

    void format(const FormatSettings & settings, FormatContext & context) override;

private:
    std::vector<Pair> values;
};

class JSONNull : public IItem
{
public:
    void format(const FormatSettings & settings, FormatContext & context) override;
};

}
