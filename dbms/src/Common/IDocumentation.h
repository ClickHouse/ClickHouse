#pragma once

#include <string>
#include <memory>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Add your language.
#define FOR_EACH_LANGUAGE(M) \
    M(en, EN) \
    M(ru, RU) \
    M(zh, ZH) \
    M(ja, JA) \
    M(fa, FA) \


enum class DocumentationLanguage
{
#define LANG_ENUM_ELEMENT(NAME, NAME_UC) NAME,
    FOR_EACH_LANGUAGE(LANG_ENUM_ELEMENT)
#undef LANG_ENUM_ELEMENT
};


inline DocumentationLanguage languageFromName(std::string code)
{
#define BRANCH(NAME, NAME_UC) if (code == #NAME) return DocumentationLanguage::NAME;
    FOR_EACH_LANGUAGE(BRANCH)
#undef BRANCH
    throw Exception("Unknown documentation language: " + code, ErrorCodes::BAD_ARGUMENTS);
}

inline std::string languageToName(DocumentationLanguage lang)
{
    switch (lang)
    {
#define BRANCH(NAME, NAME_UC) case DocumentationLanguage::NAME: return #NAME;
    FOR_EACH_LANGUAGE(BRANCH)
#undef BRANCH
    }
    __builtin_unreachable();
}


/** Examples:
  *
  * struct Documentation : IDocumentation
  * {
  *     std::string getEN() const override
  *     {
  *         return R"(Hello, world!)";
  *     }
  *
  *     std::string getRU() const override
  *     {
  *         // Included markdown file will start with return R"(. That's normal.
  *         #include "doc/ru/hello.md"
  *     }
  * };
  */

struct IDocumentation
{
    std::string get(DocumentationLanguage lang) const
    {
        switch (lang)
        {
#define BRANCH(NAME, NAME_UC) case DocumentationLanguage::NAME: return get ## NAME_UC();
    FOR_EACH_LANGUAGE(BRANCH)
#undef BRANCH
        }
        __builtin_unreachable();
    }

    /// The function must return the documentation in Markdown format.
    virtual std::string getEN() const = 0;

    /// English is default language.
    virtual std::string getRU() const { return getEN(); }
    virtual std::string getZH() const { return getEN(); }
    virtual std::string getJA() const { return getEN(); }
    virtual std::string getFA() const { return getEN(); }

    /// Documentation items will be ordered by this key (ascending).
    virtual int order() const { return 0; }

    virtual ~IDocumentation() {}
};

using DocumentationPtr = std::unique_ptr<IDocumentation>;


/// Documentation from a single string in english.
struct SimpleDocumentation : IDocumentation
{
    std::string value;
    SimpleDocumentation(std::string value_) : value(std::move(value_)) {}
    std::string getEN() const override { return value; }
};

}
