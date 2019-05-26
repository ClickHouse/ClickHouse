#pragma once

#include <string>
#include <vector>
#include <boost/noncopyable.hpp>

struct UCollator;

class Collator : private boost::noncopyable
{
public:
    explicit Collator(const std::string & locale_);
    ~Collator();

    int compare(const char * str1, size_t length1, const char * str2, size_t length2) const;

    const std::string & getLocale() const;

    static std::vector<std::string> getAvailableCollations();

private:
    std::string locale;
    UCollator * collator;
};
