#include <memory>
#include <string>

#include <snappy.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadBufferFromAlluxio.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Stopwatch.h>

void test()
{
    using namespace DB;

    String host = "localhost";
    int port = 39999;
    String path = "/the/path/to/file";

    auto in = std::make_shared<ReadBufferFromAlluxio>(host, port, path);
    assert(in != nullptr);

    Stopwatch watch;
    String output;
    WriteBufferFromString out(output);
    copyData(*in, out);
    std::cout << output << std::endl;
}

int main()
{
    test();
    return 0;
}
