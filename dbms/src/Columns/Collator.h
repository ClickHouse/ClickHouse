#pragma once

#include <string>
#include <vector>
#include <boost/noncopyable.hpp>
#include <memory>
#include <mutex>

struct UCollator;

/// Class represents available locales for collations.
class AvailableCollationLocales : private boost::noncopyable
{
public:

    struct LocaleAndLanguage
    {
        std::string locale_name; /// in ISO format
        std::string language; /// in English
    };

    static AvailableCollationLocales & instance();

    /// Get all collations with names
    const std::vector<LocaleAndLanguage> & getAvailableCollations() const;

    /// Check that collation is supported
    bool isCollationSupported(const std::string & s) const;

private:
    static std::once_flag init_flag;
    static std::unique_ptr<AvailableCollationLocales> instance_impl;
    static void init();
private:
    std::vector<LocaleAndLanguage> available_collation_locales;
};

class Collator : private boost::noncopyable
{
public:
    explicit Collator(const std::string & locale_);
    ~Collator();

    int compare(const char * str1, size_t length1, const char * str2, size_t length2) const;

    const std::string & getLocale() const;
private:

    std::string locale;
    UCollator * collator;
};
