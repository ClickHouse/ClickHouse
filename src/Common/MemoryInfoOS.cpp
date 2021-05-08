#if defined(OS_LINUX)

#include <string>
#include <unordered_map>
#include <utility>

#include "MemoryInfoOS.h"

#include <Core/Types.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

namespace DB 
{

static constexpr auto meminfo_filename = "/proc/meminfo";

static constexpr size_t READ_BUFFER_BUF_SIZE = (64 << 10);

void readStringUntilWhitespaceAndSkipWhitespaceIfAny(String & s, ReadBuffer & buf)
{
    readStringUntilWhitespace(s, buf);
    skipWhitespaceIfAny(buf);
}

template<typename T>
void readIntTextAndSkipWhitespaceIfAny(T & x, ReadBuffer & buf) 
{
    readIntText(x, buf);
    skipWhitespaceIfAny(buf);
}

MemoryInfoOS::MemoryInfoOS() 
    : meminfo_in(meminfo_filename, READ_BUFFER_BUF_SIZE, O_RDONLY | O_CLOEXEC)
{}

MemoryInfoOS::~MemoryInfoOS() {}

MemoryInfoOS::Data MemoryInfoOS::get() 
{
    meminfo_in.seek(0, SEEK_SET);
    
    MemoryInfoOS::Data data;
    String field_name;

    std::unordered_map<String, uint64_t> meminfo;

    while (!meminfo_in.eof())
        meminfo.insert(readField());
    
    data.total       = meminfo["MemTotal"];
    data.free        = meminfo["MemFree"];
    data.buffers     = meminfo["Buffers"];
    data.cached      = meminfo["Cached"];
    data.swap_total  = meminfo["SwapTotal"];
    data.swap_cached = meminfo["SwapCached"];
    data.swap_free   = meminfo["SwapFree"];

    data.free_and_cached = data.free + data.cached;

    return data;
}

std::pair<String, uint64_t> MemoryInfoOS::readField()
{
    String key;
    uint64_t val;
    
    readStringUntilWhitespaceAndSkipWhitespaceIfAny(key, meminfo_in);
    readIntTextAndSkipWhitespaceIfAny(val, meminfo_in);

    // Delete the read ":" from the end
    key.pop_back();

    return std::make_pair(key, val);
}

}

#endif
