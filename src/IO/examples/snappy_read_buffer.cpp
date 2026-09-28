#include <memory>
#include <string>

#include <snappy.h>

#include <IO/ReadBufferFromString.h>
#include <IO/SnappyReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromString.h>


int main()
{
    using namespace DB;
    String str = "this is a snappy example.\n";
    String input;
    for (size_t i = 0; i < 5; i++)
        input += str;

    String input1;
    snappy::Compress(input.data(), input.size(), &input1);

    std::unique_ptr<ReadBuffer> in1 = std::make_unique<ReadBufferFromString>(input1);
    SnappyReadBuffer in2(std::move(in1));

    String output;
    WriteBufferFromString out(output);
    copyData(in2, out);

    output.resize(out.count());
    assert(input == output);
    return 0;
}
