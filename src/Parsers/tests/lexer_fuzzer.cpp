#include <map>
#include <Parsers/Lexer.h>
#include <Core/Types.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    DB::String query;
    DB::ReadBufferFromMemory in(data, size);
    readStringUntilEOF(query, in);

    DB::Lexer lexer(query.data(), query.data() + query.size());

    while (true)
    {
        DB::Token token = lexer.nextToken();

        if (token.isEnd())
            break;

        if (token.isError())
            return 1;
    }

    return 0;
}
