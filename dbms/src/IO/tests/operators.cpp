#include <IO/Operators.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>


int main(int, char **)
{
    {
        DB::WriteBufferFromFileDescriptor buf(STDOUT_FILENO);
        buf
            << "Hello, world!" << '\n'
            << DB::escape << "Hello, world!" << '\n'
            << DB::quote << "Hello, world!" << '\n'
            << DB::double_quote << "Hello, world!" << '\n'
            << DB::binary << "Hello, world!" << '\n'
            << LocalDateTime(time(nullptr)) << '\n'
            << LocalDate(time(nullptr)) << '\n'
            << 1234567890123456789UL << '\n'
            << DB::flush;
    }

    {
        std::string hello;
        {
            DB::WriteBufferFromString buf(hello);
            buf << "Hello";
            std::cerr << hello.size() << '\n';
        }

        std::cerr << hello.size() << '\n';
        std::cerr << hello << '\n';
    }

    return 0;
}
