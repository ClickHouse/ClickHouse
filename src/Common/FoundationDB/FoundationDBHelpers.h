#pragma once

/// Help functions for extending the fdb c api

#include <cstdint>
#include <memory>
#include <string_view>
#include <base/hex.h>
#include <foundationdb/fdb_c.h>
#include <Common/StringUtils/StringUtils.h>

/// Expand std::string to key parameters.
/// In the FoundationDB C API, key are expressed as sequential parameters.
#define FDB_KEY_FROM_STRING(str) reinterpret_cast<const uint8_t *>((str).data()), static_cast<int>((str).size())

/// Expand std::string to value parameters
/// In the FoundationDB C API, value are expressed as sequential parameters.
#define FDB_VALUE_FROM_STRING(str) reinterpret_cast<const uint8_t *>((str).data()), static_cast<int>((str).size())
#define FDB_VALUE_FROM_POD(pod) reinterpret_cast<const uint8_t *>(&pod), sizeof(pod)

#define FDB_KEYSEL_LAST_LESS_THAN_STRING(str) \
    FDB_KEYSEL_LAST_LESS_THAN(reinterpret_cast<const uint8_t *>((str).data()), static_cast<int>((str).size()))
#define FDB_KEYSEL_LAST_LESS_OR_EQUAL_STRING(str) \
    FDB_KEYSEL_LAST_LESS_OR_EQUAL(reinterpret_cast<const uint8_t *>((str).data()), static_cast<int>((str).size()))
#define FDB_KEYSEL_FIRST_GREATER_THAN_STRING(str) \
    FDB_KEYSEL_FIRST_GREATER_THAN(reinterpret_cast<const uint8_t *>((str).data()), static_cast<int>((str).size()))
#define FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(str) \
    FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(reinterpret_cast<const uint8_t *>((str).data()), static_cast<int>((str).size()))

namespace DB
{
template <class FDBObject>
constexpr inline void (*FDBObjectDeleter)(FDBObject *);

/// Manage fdb object in shared_ptr
template <class FDBObject>
inline std::shared_ptr<FDBObject> fdb_manage_object(FDBObject * obj);

#define FDB_OBJECT_DELETER(FDBObject, Deleter) \
    template <> \
    inline std::shared_ptr<FDBObject> fdb_manage_object(FDBObject * obj) \
    { \
        return obj ? std::shared_ptr<FDBObject>(obj, Deleter) : std::shared_ptr<FDBObject>(nullptr); \
    }

FDB_OBJECT_DELETER(FDBDatabase, fdb_database_destroy);
FDB_OBJECT_DELETER(FDBTransaction, fdb_transaction_destroy);
FDB_OBJECT_DELETER(FDBFuture, fdb_future_destroy);

inline std::string fdb_print_key(const std::string_view & value)
{
    int non_printables = 0;
    int num_backslashes = 0;

    for (const auto & byte : value)
    {
        if (!isPrintableASCII(byte))
        {
            ++non_printables;
        }
        else if (byte == '\\')
        {
            ++num_backslashes;
        }
    }

    std::string result;
    result.reserve(value.size() + non_printables * 3 + num_backslashes);

    if (non_printables == 0 && num_backslashes == 0)
    {
        result = value;
    }
    else
    {
        for (const auto & byte : value)
        {
            if (byte == '\\')
            {
                result.push_back('\\');
                result.push_back('\\');
            }
            else if (isPrintableASCII(byte))
            {
                result.push_back(byte);
            }
            else
            {
                result.push_back('\\');
                result.push_back('x');
                const auto & u8_byte = reinterpret_cast<const uint8_t &>(byte);
                result.push_back(hexDigitLowercase(u8_byte / 16));
                result.push_back(hexDigitLowercase(u8_byte % 16));
            }
        }
    }

    return result;
}

inline std::string fdb_print_key(const void * key, int key_length)
{
    return fdb_print_key(std::string_view(reinterpret_cast<const char *>(key), key_length));
}

struct FDBVersionstamp
{
    uint8_t bytes[10];
};
}
