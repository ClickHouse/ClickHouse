#include <zlib.h>

int main()
{
    Bytef source[4096];
    Bytef compressed[8192];
    Bytef restored[4096];
    for (unsigned i = 0; i != sizeof(source); ++i)
        source[i] = static_cast<Bytef>((i * 13 + i / 37) % 43);

    uLongf compressed_size = sizeof(compressed);
    if (compress2(compressed, &compressed_size, source, sizeof(source), Z_BEST_COMPRESSION) != Z_OK)
        return 1;
    if (compressed_size >= sizeof(source))
        return 2;

    uLongf restored_size = sizeof(restored);
    if (uncompress(restored, &restored_size, compressed, compressed_size) != Z_OK)
        return 3;
    if (restored_size != sizeof(source))
        return 4;
    for (unsigned i = 0; i != sizeof(source); ++i)
        if (source[i] != restored[i])
            return 5;
    return 0;
}
