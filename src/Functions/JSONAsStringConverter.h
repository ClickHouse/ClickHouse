#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Interpreters/Context_fwd.h>

#include <memory>
#include <type_traits>

class SipHash;

namespace DB
{

struct FormatSettings;

template <typename T, typename = void>
struct ImplSupportsJSONHaystack : std::false_type {};

template <typename T>
struct ImplSupportsJSONHaystack<T, std::void_t<decltype(T::supports_json_haystack)>> : std::bool_constant<T::supports_json_haystack> {};

/// Converts a `JSON` column to the text `CAST(json AS String)` gives in the session of `context`.
class JSONAsStringConverter
{
public:
    explicit JSONAsStringConverter(const ContextPtr & context);

    ColumnPtr convert(const ColumnWithTypeAndName & json) const;
    void updateHash(SipHash & hash) const;

private:
    std::shared_ptr<const FormatSettings> format_settings;
    UInt64 format_settings_hash = 0;
};

}
