#pragma once
#include <unordered_map>
#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <mutex>

struct UConverter;

namespace DB
{
class MySQLCharset final : boost::noncopyable
{
public:
    ~MySQLCharset();
    String getCharsetFromId(UInt32 id);
    Int32 convertFromId(UInt32 id, String & to, const String & from);
    bool needConvert(UInt32 id);
private:
    std::mutex mutex;
    std::unordered_map<String, UConverter *> conv_cache;
    UConverter * getCachedConverter(const String & charset);
    static const std::unordered_map<Int32, String> charsets;
};

using MySQLCharsetPtr = std::shared_ptr<MySQLCharset>;
}
