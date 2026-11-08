#include <Storages/Kafka/Clients/Types.h>

namespace Kafka
{

String Uuid::toString() const
{
    static constexpr char hex[] = "0123456789abcdef";
    String out;
    out.reserve(36);
    for (size_t i = 0; i < 16; ++i)
    {
        if (i == 4 || i == 6 || i == 8 || i == 10)
            out.push_back('-');
        out.push_back(hex[data[i] >> 4]);
        out.push_back(hex[data[i] & 0x0F]);
    }
    return out;
}

}
