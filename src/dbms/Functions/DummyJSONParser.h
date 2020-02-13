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

    bool parse(const StringRef &) { throw Exception{"Functions JSON* are not supported without AVX2", ErrorCodes::NOT_IMPLEMENTED}; }

    using Iterator = std::nullptr_t;
    Iterator getRoot() const { return nullptr; }

    static bool isInt64(const Iterator &) { return false; }
    static bool isUInt64(const Iterator &) { return false; }
    static bool isDouble(const Iterator &) { return false; }
    static bool isString(const Iterator &) { return false; }
    static bool isArray(const Iterator &) { return false; }
    static bool isObject(const Iterator &) { return false; }
    static bool isBool(const Iterator &) { return false; }
    static bool isNull(const Iterator &) { return true; }

    static Int64 getInt64(const Iterator &) { return 0; }
    static UInt64 getUInt64(const Iterator &) { return 0; }
    static double getDouble(const Iterator &) { return 0; }
    static bool getBool(const Iterator &) { return false; }
    static StringRef getString(const Iterator &) { return {}; }

    static size_t sizeOfArray(const Iterator &) { return 0; }
    static bool firstArrayElement(Iterator &) { return false; }
    static bool arrayElementByIndex(Iterator &, size_t) { return false; }
    static bool nextArrayElement(Iterator &) { return false; }

    static size_t sizeOfObject(const Iterator &) { return 0; }
    static bool firstObjectMember(Iterator &) { return false; }
    static bool firstObjectMember(Iterator &, StringRef &) { return false; }
    static bool objectMemberByIndex(Iterator &, size_t) { return false; }
    static bool objectMemberByName(Iterator &, const StringRef &) { return false; }
    static bool nextObjectMember(Iterator &) { return false; }
    static bool nextObjectMember(Iterator &, StringRef &) { return false; }
    static bool isObjectMember(const Iterator &) { return false; }
    static StringRef getKey(const Iterator &) { return {}; }
};

}
