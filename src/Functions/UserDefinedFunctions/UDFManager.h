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

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>

#include "UDFControlCommand.h"
#include "UDFLib.h"


namespace DB
{

class UDFManager : private boost::noncopyable
{
public:
    UDFManager() : in(std::cin, 16), out(std::cout, 16) { }
    ~UDFManager() = default;

    int run();

private:
    UDFControlCommandResult initLib(const std::string & filename);

    ReadBufferFromIStream in;
    WriteBufferFromOStream out;
    std::map<std::string, UDFLib> libs;
};

}

#endif //CLICKHOUSE_UDFMANAGER_H
