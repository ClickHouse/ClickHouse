#include <iostream>
#include <memory>
#include <string>

#include <snappy.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/HadoopSnappyReadBuffer.h>

std::string uncompress(size_t buf_size)
{
    using namespace DB;

    String path = "test.snappy";
    std::unique_ptr<ReadBuffer> in1 = std::make_unique<ReadBufferFromFile>(path, buf_size);
    HadoopSnappyReadBuffer in2(std::move(in1));

    String output;
    WriteBufferFromString out(output);
    copyData(in2, out);

    output.resize(out.count());
    return output;
}

int main()
{
    auto output = uncompress(1024 * 1024);
    for (size_t i = 1; i < 1024; ++i)
    {
        size_t buf_size = 1024 * i;
        if (uncompress(buf_size) != output)
        {
            std::cout << "test hadoop snappy read buffer failed, buf_size:" << buf_size << std::endl;
            return 1;
        }
    }
    if (uncompress(256) != output)
    {
        std::cout << "test hadoop snappy read buffer failed, buf_size:" << 256 << std::endl;
        return 1;
    }
    std::cout << "test hadoop snappy read buffer success" << std::endl;
    return 0;
}
