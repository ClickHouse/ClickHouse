#include <Client/BuzzHouse/Utils/MD5Impl.h>
#include <Common/Exception.h>

#include <fstream>

namespace DB
{
namespace ErrorCodes
{
extern const int BUZZHOUSE;
}
}

namespace BuzzHouse
{

void MD5Impl::hashFile(const String & filePath, Poco::DigestEngine::Digest & res)
{
    std::ifstream file(filePath, std::ios::binary);

    if (!file)
    {
        throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Could not open file: {}", filePath);
    }
    while (file.read(reinterpret_cast<char *>(inputBuffer), inputBufferSize) || file.gcount() > 0)
    {
        ctx.update(reinterpret_cast<const uint8_t *>(inputBuffer), file.gcount());
    }
    res = ctx.digest();
}

}
