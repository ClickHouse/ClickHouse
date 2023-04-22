#pragma once

#include <base/types.h>
#include <map>
#include "config.h"

namespace DB
{

#if USE_ENET
class ENetPack
{
    public:
        void set(std::string key, std::string val)
        {
            data[key] = val;
        }

        std::string get(std::string key) const
        {
            if (data.find(key) != data.end())
            {
                return data.at(key);
            }
            return "";
        }

        std::string get(std::string key, std::string def) const
        {
            if (data.find(key) != data.end())
            {
                return data.at(key);
            }
            return def;
        }

        std::string serialize() const
        {
            std::string res = "";
            for (auto pair: data)
            {
                res += pair.first + "=" + pair.second + "\t";
            }
            return res;
        }

        void deserialize(const char * inp)
        {
            std::string res(inp);
            size_t i = 0;
            std::string prev = "";
            std::string cur = "";
            while (i < res.size())
            {
                if (res[i] == '=')
                {
                    prev = cur;
                    cur = "";
                }
                else if (res[i] == '\t')
                {
                    data[prev] = cur;
                    cur = "";
                    prev = "";
                }
                else
                {
                    cur += res[i];
                }
                ++i;
            }
        }

    public:
        std::map<std::string, std::string> data;
};
#endif

}
