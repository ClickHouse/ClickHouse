#pragma once

#include <string>
#include <optional>
#include <vector>
#include <boost/noncopyable.hpp>
#include <unordered_map>


struct UCollator;

/// Class represents available locales for collations.
class AvailableCollationLocales : private boost::noncopyable
{
public:

    struct LocaleAndLanguage
    {
        std::string locale_name; /// ISO locale code
        std::optional<std::string> language; /// full language name in English
    };

    using AvailableLocalesMap = std::unordered_map<std::string, LocaleAndLanguage>;
    using LocalesVector = std::vector<LocaleAndLanguage>;

    static const AvailableCollationLocales & instance();

    /// Get all collations with names in sorted order
    LocalesVector getAvailableCollations() const;

    /// Check that collation is supported
    bool isCollationSupported(const std::string & locale_name) const;

private:
    AvailableCollationLocales();

    AvailableLocalesMap locales_map;
};

class Collator : private boost::noncopyable
{
public:
    explicit Collator(const std::string & locale_);
    ~Collator();

    int compare(const char * str1, size_t length1, const char * str2, size_t length2) const;

    const std::string & getLocale() const;

    bool operator==(const Collator & other) const { return this->getLocale() == other.getLocale(); }

private:
    std::string locale;
    UCollator * collator;
};
