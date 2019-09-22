#include <string>

#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>

#include <Core/Types.h>
#include <Common/Stopwatch.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    try {
        std::string Str(reinterpret_cast<const char *>(data), size);

        DB::ReadBufferFromString from(Str);
        DB::CompressedReadBuffer in{from};
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
