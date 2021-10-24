#include <iostream>
#include <memory>
#include <sstream>

#include <snappy.h>

#include <IO/SnappyWriteBuffer.h>
#include <IO/WriteBufferFromOStream.h>
#include <Common/Exception.h>
#include <base/types.h>

int main()
{
    using namespace DB;

    try
    {
        String str = "this is a snappy example.\n";
        String input;
        for (size_t i = 0; i < 5; i++)
            input += str;
        std::cout << "input: " << input << std::endl;

        std::stringstream s;       // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        s.exceptions(std::ios::badbit);
        {
            std::unique_ptr<WriteBuffer> sink = std::make_unique<WriteBufferFromOStream>(s);
            SnappyWriteBuffer buf(std::move(sink));
            buf.write(input.data(), input.size());
            buf.next();
        }

        String output = s.str();
        String expect;
        bool success = snappy::Uncompress(output.data(), output.size(), &expect);
        assert(success);
        std::cout << "expect: " << expect << std::endl;
        assert(input == expect);
    }
    catch (const Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }
}
