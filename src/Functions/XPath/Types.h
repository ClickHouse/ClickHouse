#pragma once

#include <base/find_symbols.h>
#include <Common/StringUtils.h>

#include <memory>
#include <string>
#include <vector>

namespace DB
{

struct CaseInsensitiveStringView
{
    std::string_view value;

    CaseInsensitiveStringView() = default;
    explicit CaseInsensitiveStringView(std::string_view value_)
        : value(value_)
    {
    }

    CaseInsensitiveStringView& operator=(std::string_view value_)
    {
        value = value_;
        return *this;
    }

    bool operator==(const CaseInsensitiveStringView & other) const
    {
        if (value.size() != other.value.size())
            return false;
        for (size_t i = 0; i < value.size(); ++i)
        {
            if (!equalsCaseInsensitive(value[i], other.value[i]))
                return false;
        }

        return true;
    }

    bool operator!=(const CaseInsensitiveStringView & other) const { return !(*this == other); }
};

struct TagPreview
{
    CaseInsensitiveStringView name;
    bool is_closing = false;
};

struct Attribute
{
    CaseInsensitiveStringView key;
    std::string_view value;
};

class IValueMatcher
{
public:
    virtual bool match(std::string_view value) const = 0;
    virtual ~IValueMatcher() = default;
};

using ValueMatcherPtr = std::shared_ptr<IValueMatcher>;

class ITagScanner
{
    const char * last_tag_start = nullptr;

public:
    virtual const char * scan(const char * begin, const char * end, TagPreview & result) = 0;

    const char * getLastTagStart() { return last_tag_start; }

    void setLastTagStart(const char * new_start) { last_tag_start = new_start; }

    virtual ~ITagScanner() = default;
};

using TagScannerPtr = std::shared_ptr<ITagScanner>;


struct Document
{
    const char * begin = nullptr;
    const char * end = nullptr;
};

class IInstruction
{
public:
    virtual void apply(const std::vector<Document> & docs, std::vector<std::vector<Document>> & res, TagScannerPtr tag_scanner) = 0;
    virtual String getName() = 0;
    virtual ~IInstruction() = default;
};

using InstructionPtr = std::shared_ptr<IInstruction>;

}
