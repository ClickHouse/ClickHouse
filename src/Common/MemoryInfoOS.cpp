#if defined(OS_LINUX)

#include <unistd.h>
#include <cassert>
#include <string>

#include "MemoryInfoOS.h"

#include <Core/Types.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

namespace DB 
{

static constexpr auto meminfo_filename = "/proc/meminfo";
    
MemoryInfoOS::MemoryInfoOS() 
    : meminfo_in(meminfo_filename, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY | O_CLOEXEC)
{}

MemoryInfoOS::~MemoryInfoOS() {}

MemoryInfoOS::Data MemoryInfoOS::get() 
{
    meminfo_in.seek(0, SEEK_SET);
    
    MemoryInfoOS::Data data;
    String field_name;

    assert(readField(data.total, String("MemTotal")));
    assert(readField(data.free, String("MemFree")));
    skipField();
    assert(readField(data.buffers, String("Buffers")));
    assert(readField(data.cached, String("Cached")));

    data.free_and_cached = data.free + data.cached;

    assert(readField(data.swap_cached, String("SwapCached")));
    
    while (!readField(data.swap_total, String("SwapTotal"))) {}

    assert(readField(data.swap_free, String("SwapFree")));

    return data;
}

bool MemoryInfoOS::readField(unsigned long & field_val, const String & field_name_target)
{
    String field_name;
    
    readStringAndSkipWhitespaceIfAny(field_name, meminfo_in);
    readIntTextAndSkipWhitespaceIfAny(field_val, meminfo_in);
    return (field_name == (field_name_target + String(":")));
}

void MemoryInfoOS::skipField() 
{
    skipToNextLineOrEOF(meminfo_in);
}

void MemoryInfoOS::readStringAndSkipWhitespaceIfAny(String & s, ReadBuffer & buf) 
{
    readString(s, buf);
    skipWhitespaceIfAny(buf);
}

template<typename T>
void MemoryInfoOS::readIntTextAndSkipWhitespaceIfAny(T & x, ReadBuffer & buf) 
{
    readIntText(x, buf);
    skipWhitespaceIfAny(buf);
}

}

#endif
