#include <Client/BuzzHouse/Utils/MD5Impl.h>

#include <fstream>

namespace BuzzHouse
{

void MD5Impl::hashFile(const String & file_path, Poco::DigestEngine::Digest & res)
{
    std::ifstream file(file_path, std::ios::binary);

    if (!file)
    {
        throw std::runtime_error("Could not open file: " + file_path);
    }
    while (file.read(reinterpret_cast<char *>(input_buffer), input_buffer_size) || file.gcount() > 0)
    {
        ctx.update(reinterpret_cast<const uint8_t *>(input_buffer), file.gcount());
    }
    res = ctx.digest();
}

}
