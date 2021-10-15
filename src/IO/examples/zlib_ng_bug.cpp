#include <unistd.h>
#include <vector>
#include <stdexcept>

#include <zlib.h>

#pragma GCC diagnostic ignored "-Wold-style-cast"


/// https://github.com/zlib-ng/zlib-ng/issues/494
int main(int, char **)
{
    std::vector<unsigned char> in(1048576);
    std::vector<unsigned char> out(1048576);

    ssize_t in_size = read(STDIN_FILENO, in.data(), 1048576);
    if (in_size < 0)
        throw std::runtime_error("Cannot read");
    in.resize(in_size);

    z_stream zstr{};
    if (Z_OK != deflateInit2(&zstr, 1, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY))
        throw std::runtime_error("Cannot deflateInit2");

    zstr.next_in = in.data();
    zstr.avail_in = in.size();
    zstr.next_out = out.data();
    zstr.avail_out = out.size();

    while (zstr.avail_in > 0)
        if (Z_OK != deflate(&zstr, Z_NO_FLUSH))
            throw std::runtime_error("Cannot deflate");

    while (true)
    {
        int rc = deflate(&zstr, Z_FINISH);

        if (rc == Z_STREAM_END)
            break;

        if (rc != Z_OK)
            throw std::runtime_error("Cannot finish deflate");
    }

    deflateEnd(&zstr);

    if (ssize_t(zstr.total_out) != write(STDOUT_FILENO, out.data(), zstr.total_out))
        throw std::runtime_error("Cannot write");

    return 0;
}
