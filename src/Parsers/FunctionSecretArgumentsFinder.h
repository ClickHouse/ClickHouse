#pragma once

#include <vector>

namespace DB
{

class FunctionSecretArgumentsFinder
{
public:
    struct Result
    {
        /// Result constructed by default means no arguments will be hidden.
        size_t start = static_cast<size_t>(-1);
        size_t count = 0; /// Mostly it's either 0 or 1. There are only a few cases where `count` can be greater than 1 (e.g. see `encrypt`).
                            /// In all known cases secret arguments are consecutive
        bool are_named = false; /// Arguments like `password = 'password'` are considered as named arguments.
        /// E.g. "headers" in `url('..', headers('foo' = '[HIDDEN]'))`
        std::vector<std::string> nested_maps;

        bool hasSecrets() const
        {
            return count != 0 || !nested_maps.empty();
        }
    };
};

}
