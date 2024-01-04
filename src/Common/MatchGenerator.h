#pragma once

#include <base/types.h>
#include <memory>

namespace re2
{
    class Regexp;
}

namespace DB
{

class RandomStringGeneratorByRegexp
{
public:
    RandomStringGeneratorByRegexp(const String & re_str, bool logging);
    String generate() const;

private:
    struct RegexpPtrDeleter
    {
        void operator()(re2::Regexp * re) const noexcept;
    };
    using RegexpPtr = std::unique_ptr<re2::Regexp, RegexpPtrDeleter>;

    RegexpPtr regexp;
    std::function<String()> generatorFunc;
};

}
