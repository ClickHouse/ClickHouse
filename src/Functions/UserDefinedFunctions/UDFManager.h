#ifndef CLICKHOUSE_UDFMANAGER_H
#define CLICKHOUSE_UDFMANAGER_H

#include <map>
#include <iostream>

#include <unistd.h>

#include <boost/interprocess/anonymous_shared_memory.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/noncopyable.hpp>

#include <IO/ReadBufferFromIStream.h>
#include <IO/WriteBufferFromOStream.h>

#include "UDFLib.h"
#include "UDFControlCommand.h"


namespace DB
{

class UDFManager : private boost::noncopyable
{
public:
    UDFManager() : in(ReadBufferFromIStream(std::cin)), out(WriteBufferFromOStream(std::cout)) { }
    ~UDFManager() = default;

    int run();

private:
    UDFControlCommandResult initLib(const std::string & filename);

    ReadBuffer in;
    WriteBuffer out;
    std::map<std::string, UDFLib> libs;
};

}

#endif //CLICKHOUSE_UDFMANAGER_H
