#pragma once

#include <Common/Wyrd/Facade/Atomic.h>

#include <base/types.h>

#include <boost/noncopyable.hpp>

#include <string>

namespace DB::Wyrd
{

class ExclusiveScope
{
    class Section : private boost::noncopyable
    {
    public:
        Section(ExclusiveScope & scope_, std::string section_name_);
        ~Section();

    private:
        ExclusiveScope & scope;
        const std::string section_name;
    };

public:
    explicit ExclusiveScope(std::string name_);

    [[nodiscard]] Section enter(const std::string & section_name);

private:
    const std::string name;
    Atomic<UInt32> inside{0};
};

}
