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
};

struct FormatContext
{
    WriteBuffer & out;
    size_t offset;
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
    explicit JSONString(std::string value_) : value(std::move(value_)) {}
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
        static_assert(std::is_arithmetic_v<T>, "JSONNumber support only numberic types");
    }

    void format(const FormatSettings & settings, FormatContext & context) override
    {
        bool is_finite = isFinite(value);

        const bool need_quote = (std::is_integral_v<T> && (sizeof(T) >= 8) && settings.settings.json.quote_64bit_integers)
            || (settings.settings.json.quote_denormals && !is_finite);

        if (need_quote)
            writeChar('"', context.out);

        if (is_finite)
            writeText(value, context.out);
        else if (!settings.settings.json.quote_denormals)
            writeCString("null", context.out);
        else
            writeDenormalNumber(value, context.out);

        if (need_quote)
            writeChar('"', context.out);
    }

private:
    T value;
};

class JSONArray : public IItem
{
public:
    void add(ItemPtr value) { values.push_back(std::move(value)); }
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
    void format(const FormatSettings & settings, FormatContext & context) override;

private:
    std::vector<Pair> values;
};

}
