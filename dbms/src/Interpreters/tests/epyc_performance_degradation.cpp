#include <iostream>
#include <Core/Types.h>
#include <Common/HashTable/HashMap.h>


int main(int, char **)
{
    using namespace DB;

    HashMap<UInt64, UInt64, TrivialHash, HashTableFixedGrower<16>> map;

    map[12345] = 1;

    for (auto it = map.begin(); it != map.end(); ++it)
        std::cerr << it->first << ": " << it->second << "\n";

    return 0;
}
