#include <string>

#include <iostream>
#include <fstream>

#include <base/types.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromOStream.h>


int main(int, char **)
{
    try
    {
        DB::Int64 a = -123456;
        DB::Float64 b = 123.456;
        DB::String c = "вася пе\tтя";
        DB::String d = "'xyz\\";

        std::ofstream s("test");
        DB::WriteBufferFromOStream out(s);

        for (int i = 0; i < 1000000; ++i)
        {
            DB::writeIntText(a, out);
            DB::writeChar(' ', out);

            DB::writeFloatText(b, out);
            DB::writeChar(' ', out);

            DB::writeEscapedString(c, out);
            DB::writeChar('\t', out);

            DB::writeQuotedString(d, out);
            DB::writeChar('\n', out);
        }

        out.finalize();
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
