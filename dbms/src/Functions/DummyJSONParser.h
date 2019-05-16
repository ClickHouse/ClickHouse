#pragma once

#include <common/StringRef.h>
#include <Common/Exception.h>
#include <Core/Types.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// This class can be used as an argument for the template class FunctionJSON when we unable to parse JSONs.
/// It can't do anything useful and just throws an exception.
struct DummyJSONParser
{
    static constexpr bool need_preallocate = false;
    void preallocate(size_t) {}
    bool parse(const char *, size_t) { throw Exception{"Functions JSON* are not supported without AVX2", ErrorCodes::NOT_IMPLEMENTED}; }

    using Iterator = std::nullptr_t;
    Iterator getRoot() const { return nullptr; }

    static bool downToArray(Iterator &) { return false; }
    static bool downToObject(Iterator &) { return false; }
    static bool downToObject(Iterator &, StringRef &) { return false; }
    static bool parentScopeIsObject(const Iterator &) { return false; }
    static bool next(Iterator &) { return false; }
    static bool nextKeyValue(Iterator &) { return false; }
    static bool nextKeyValue(Iterator &, StringRef &) { return false; }
    static bool isInt64(const Iterator &) { return false; }
    static bool isUInt64(const Iterator &) { return false; }
    static bool isDouble(const Iterator &) { return false; }
    static bool isString(const Iterator &) { return false; }
    static bool isArray(const Iterator &) { return false; }
    static bool isObject(const Iterator &) { return false; }
    static bool isBool(const Iterator &) { return false; }
    static bool isNull(const Iterator &) { return false; }
    static StringRef getKey(const Iterator &) { return {}; }
    static StringRef getString(const Iterator &) { return {}; }
    static Int64 getInt64(const Iterator &) { return 0; }
    static UInt64 getUInt64(const Iterator &) { return 0; }
    static double getDouble(const Iterator &) { return 0; }
    static bool getBool(const Iterator &) { return false; }
};

}
