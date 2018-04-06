#include <Common/iostream_debug_helpers.h>

#include <iostream>
#include <memory>
#include <vector>
#include <map>
#include <tuple>
#include <array>
#include <utility>


struct S1;
struct S2 {};

int main(int, char **)
{
    int x = 1;

    DUMP(x);
    DUMP(x, 1, &x);

    DUMP(std::make_unique<int>(1));
    DUMP(std::make_shared<int>(1));

    std::vector<int> vec{1, 2, 3};
    DUMP(vec);

    auto pair = std::make_pair(1, 2);
    DUMP(pair);

    auto tuple = std::make_tuple(1, 2, 3);
    DUMP(tuple);

    std::map<int, std::string> map{{1, "hello"}, {2, "world"}};
    DUMP(map);

    std::initializer_list<const char *> list{"hello", "world"};
    DUMP(list);

    std::array<const char *, 2> arr{{"hello", "world"}};
    DUMP(arr);

    //DUMP([]{});

    S1 * s = nullptr;
    DUMP(s);

    DUMP(S2());

    return 0;
}
