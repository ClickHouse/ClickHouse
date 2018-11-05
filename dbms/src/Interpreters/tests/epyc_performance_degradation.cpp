#include <iostream>
#include <cstdint>


int main(int, char **)
{
    static constexpr size_t size = 0x10000;
    using Value = std::pair<uint64_t, uint64_t>;
    Value * map = static_cast<Value *>(calloc(size * sizeof(Value), 1));
    if (!map)
        return 1;

    map[12345] = {12345, 1};

    for (auto it = map; it != map + size; ++it)
        if (it->first)
            std::cerr << it->first << ": " << it->second << "\n";

    return 0;
}
