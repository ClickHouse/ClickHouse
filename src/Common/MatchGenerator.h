#pragma once

#include <functional>
#include <memory>
#include <base/types.h>
#include <pcg_random.hpp>

namespace re2
{
    class Regexp;
}

namespace DB
{

class RandomStringGeneratorByRegexp
{
public:
    explicit RandomStringGeneratorByRegexp(const String & re_str);
    String generate() const;
    String generate(pcg64 & rng) const;

private:
    struct RegexpPtrDeleter
    {
        void operator()(re2::Regexp * re) const noexcept;
    };
    using RegexpPtr = std::unique_ptr<re2::Regexp, RegexpPtrDeleter>;

    RegexpPtr regexp;
    std::function<String(pcg64 *)> generatorFunc;
};

}
