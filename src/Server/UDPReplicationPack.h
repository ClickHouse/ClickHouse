#pragma once

#include <base/types.h>
#include <map>
#include "config.h"

namespace DB
{

class UDPReplicationPack
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
                res += pair.first + "=" + pair.second + '\t';
            }
            return res;
        }

        std::string deserialize(const char * inp, size_t size)
        {
            size_t i = 0;
            std::string prev = "";
            std::string cur = "";
            while (i < size && inp[i] != '\r')
            {
                if (inp[i] == '=')
                {
                    prev = cur;
                    cur = "";
                }
                else if (inp[i] == '\t')
                {
                    data[prev] = cur;
                    cur = "";
                    prev = "";
                }
                else
                {
                    cur += inp[i];
                }
                ++i;
            }
            std::string payload = "";
            if (i < size)
            {
                payload = std::string(&inp[i + 1], size - i - 2);
            }
            return payload;
        }

    public:
        std::map<std::string, std::string> data;
};

}
