#pragma once
#if defined(OS_LINUX)

#include <cstdint>
#include <string>

#include <Core/Types.h>

#include <IO/ReadBufferFromFile.h>

namespace DB 
{

/** Opens file /proc/meminfo. Keeps it open and reads statistics about memory usage.
  * This is Linux specific.
  * See: man procfs
  */

class MemoryInfoOS 
{
public:
    // In kB
    struct Data {
        unsigned long total;
        unsigned long free;
        unsigned long buffers;
        unsigned long cached;
        unsigned long free_and_cached;

        unsigned long swap_total;
        unsigned long  swap_free;
        unsigned long swap_cached;
    };

    MemoryInfoOS();
    ~MemoryInfoOS();

    Data get();

private:
    ReadBufferFromFile meminfo_in;
    
    bool readField(unsigned long & field_val, const String & field_name_target);

    void skipField();
    
    static void readStringAndSkipWhitespaceIfAny(String & s, ReadBuffer & buf);

    template<typename T>
    static void readIntTextAndSkipWhitespaceIfAny(T & x, ReadBuffer & buf);
};

}

#endif
