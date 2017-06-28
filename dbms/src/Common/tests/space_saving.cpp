#include <iomanip>
#include <iostream>
#include <map>
#include <string>

#include <Common/SpaceSaving.h>
#include <common/StringRef.h>

int main(int argc, char ** argv)
{
    {
        using Cont = DB::SpaceSaving<int>;
        Cont first(10);

        /* Test biased insertion */

        for (int i = 0; i < 200; ++i)
        {
            first.insert(i);
            int k = i % 5; // Bias towards 0-4
            first.insert(k);
        }

        /* Test whether the biased elements are retained */

        std::map<int, UInt64> expect;
        for (int i = 0; i < 5; ++i)
        {
            expect[i] = 41;
        }

        for (auto x : first.topK(5))
        {
            if (expect[x.key] != x.count)
            {
                std::cerr << "key: " << x.key << " value: " << x.count << " expected: " << expect[x.key] << std::endl;
            }
            else
            {
                std::cout << "key: " << x.key << " value: " << x.count << std::endl;
            }
            expect.erase(x.key);
        }

        if (!expect.empty())
        {
            std::cerr << "expected to find all heavy hitters" << std::endl;
        }

        /* Create another table and test merging */

        Cont second(10);
        for (int i = 0; i < 200; ++i)
        {
            first.insert(i);
        }

        for (int i = 0; i < 5; ++i)
        {
            expect[i] = 42;
        }

        first.merge(second);

        for (auto x : first.topK(5))
        {
            if (expect[x.key] != x.count)
            {
                std::cerr << "key: " << x.key << " value: " << x.count << " expected: " << expect[x.key] << std::endl;
            }
            else
            {
                std::cout << "key: " << x.key << " value: " << x.count << std::endl;
            }
            expect.erase(x.key);
        }
    }

    {
        /* Same test for string keys */

        using Cont = DB::SpaceSaving<StringRef, StringRefHash>;
        Cont cont(10);

        for (int i = 0; i < 400; ++i)
        {
            cont.insert(std::to_string(i));
            cont.insert(std::to_string(i % 5)); // Bias towards 0-4
        }

        // The hashing is going to be more lossy
        // Expect at least ~ 10% count
        std::map<std::string, UInt64> expect;
        for (int i = 0; i < 5; ++i)
        {
            expect[std::to_string(i)] = 38;
        }

        for (auto x : cont.topK(5))
        {
            auto key = x.key.toString();
            if (x.count < expect[key])
            {
                std::cerr << "key: " << key << " value: " << x.count << " expected: " << expect[key] << std::endl;
            }
            else
            {
                std::cout << "key: " << key << " value: " << x.count << std::endl;
            }
            expect.erase(key);
        }

        if (!expect.empty())
        {
            std::cerr << "expected to find all heavy hitters" << std::endl;
            abort();
        }
    }

    return 0;
}
