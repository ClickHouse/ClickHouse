#ifndef CLICKHOUSE_UDFSHAREDMEMORY_H
#define CLICKHOUSE_UDFSHAREDMEMORY_H

#include <boost/interprocess/shared_memory_object.hpp>


class UDFSharedMemory
{
    template <class OpenTag, class AccessTag>
    UDFSharedMemory(std::string_view name, OpenTag openTag, AccessTag accessTag) : memory(accessTag, name, openTag) { }

    boost::interprocess::shared_memory_object memory;
};


#endif //CLICKHOUSE_UDFSHAREDMEMORY_H
