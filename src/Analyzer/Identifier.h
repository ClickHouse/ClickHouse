#pragma once

#include <vector>
#include <string>

#include <fmt/core.h>
#include <fmt/format.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/join.hpp>


namespace DB
{

/** Identifier consists from identifier parts.
  * Each identifier part is arbitrary long sequence of digits, underscores, lowercase and uppercase letters.
  * Example: a, a.b, a.b.c.
  */
class Identifier
{
public:
    Identifier() = default;

    /// Create Identifier from parts
    explicit Identifier(const std::vector<std::string> & parts_)
        : parts(parts_)
        , full_name(boost::algorithm::join(parts, "."))
    {
    }

    /// Create Identifier from parts
    explicit Identifier(std::vector<std::string> && parts_)
        : parts(std::move(parts_))
        , full_name(boost::algorithm::join(parts, "."))
    {
    }

    /// Create Identifier from full name, full name is split with '.' as separator.
    explicit Identifier(const std::string & full_name_)
        : full_name(full_name_)
    {
        boost::split(parts, full_name, [](char c) { return c == '.'; });
    }

    /// Create Identifier from full name, full name is split with '.' as separator.
    explicit Identifier(std::string && full_name_)
        : full_name(std::move(full_name_))
    {
        boost::split(parts, full_name, [](char c) { return c == '.'; });
    }

    const std::string & getFullName() const
    {
        return full_name;
    }

    const std::vector<std::string> & getParts() const
    {
        return parts;
    }

    size_t getPartsSize() const
    {
        return parts.size();
    }

    bool empty() const
    {
        return parts.empty();
    }

    bool isEmpty() const
    {
        return parts.empty();
    }

    bool isShort() const
    {
        return parts.size() == 1;
    }

    bool isCompound() const
    {
        return parts.size() > 1;
    }

    const std::string & at(size_t index) const
    {
        if (index >= parts.size())
            throw std::out_of_range("identifier access part is out of range");

        return parts[index];
    }

    const std::string & operator[](size_t index) const
    {
        return parts[index];
    }

    const std::string & front() const
    {
        return parts.front();
    }

    const std::string & back() const
    {
        return parts.back();
    }

    /// Returns true, if identifier starts with part, false otherwise
    bool startsWith(const std::string_view & part)
    {
        return !parts.empty() && parts[0] == part;
    }

    /// Returns true, if identifier ends with part, false otherwise
    bool endsWith(const std::string_view & part)
    {
        return !parts.empty() && parts.back() == part;
    }

    using const_iterator = std::vector<std::string>::const_iterator;

    const_iterator begin() const
    {
        return parts.begin();
    }

    const_iterator end() const
    {
        return parts.end();
    }

    void popFirst(size_t parts_to_remove_size)
    {
        assert(parts_to_remove_size <= parts.size());

        size_t parts_size = parts.size();
        std::vector<std::string> result_parts;
        result_parts.reserve(parts_size - parts_to_remove_size);

        for (size_t i = parts_to_remove_size; i < parts_size; ++i)
            result_parts.push_back(std::move(parts[i]));

        parts = std::move(result_parts);
        full_name = boost::algorithm::join(parts, ".");
    }

    void popFirst()
    {
        popFirst(1);
    }

    void pop_front() /// NOLINT
    {
        popFirst();
    }

    void popLast(size_t parts_to_remove_size)
    {
        assert(parts_to_remove_size <= parts.size());

        for (size_t i = 0; i < parts_to_remove_size; ++i)
        {
            size_t last_part_size = parts.back().size();
            parts.pop_back();
            bool is_not_last = !parts.empty();
            full_name.resize(full_name.size() - (last_part_size + static_cast<size_t>(is_not_last)));
        }
    }

    void popLast()
    {
        popLast(1);
    }

    void pop_back() /// NOLINT
    {
        popLast();
    }

    void push_back(std::string && part) /// NOLINT
    {
        emplace_back(std::move(part));
    }

    void push_back(const std::string & part) /// NOLINT
    {
        emplace_back(part);
    }

    template <typename ...Args>
    void emplace_back(Args&&... args) /// NOLINT
    {
        parts.emplace_back(std::forward<Args>(args)...);
        bool was_not_empty = parts.size() != 1;
        if (was_not_empty)
            full_name += '.';
        full_name += parts.back();
    }
private:
    std::vector<std::string> parts;
    std::string full_name;
};

inline bool operator==(const Identifier & lhs, const Identifier & rhs)
{
    return lhs.getFullName() == rhs.getFullName();
}

inline bool operator!=(const Identifier & lhs, const Identifier & rhs)
{
    return !(lhs == rhs);
}

inline std::ostream & operator<<(std::ostream & stream, const Identifier & identifier)
{
    stream << identifier.getFullName();
    return stream;
}

using Identifiers = std::vector<Identifier>;

/// View for Identifier
class IdentifierView
{
public:
    IdentifierView() = default;

    IdentifierView(const Identifier & identifier) /// NOLINT
        : full_name_view(identifier.getFullName())
        , parts_start_it(identifier.begin())
        , parts_end_it(identifier.end())
    {}

    std::string_view getFullName() const
    {
        return full_name_view;
    }

    size_t getPartsSize() const
    {
        return parts_end_it - parts_start_it;
    }

    bool empty() const
    {
        return parts_start_it == parts_end_it;
    }

    bool isEmpty() const
    {
        return parts_start_it == parts_end_it;
    }

    bool isShort() const
    {
        return getPartsSize() == 1;
    }

    bool isCompound() const
    {
        return getPartsSize() > 1;
    }

    std::string_view at(size_t index) const
    {
        if (index >= getPartsSize())
            throw std::out_of_range("identifier access part is out of range");

        return *(parts_start_it + index);
    }

    std::string_view operator[](size_t index) const
    {
        return *(parts_start_it + index);
    }

    std::string_view front() const
    {
        return *parts_start_it;
    }

    std::string_view back() const
    {
        return *(parts_end_it - 1);
    }

    bool startsWith(std::string_view part) const
    {
        return !isEmpty() && *parts_start_it == part;
    }

    bool endsWith(std::string_view part) const
    {
        return !isEmpty() && *(parts_end_it - 1) == part;
    }

    void popFirst(size_t parts_to_remove_size)
    {
        assert(parts_to_remove_size <= getPartsSize());

        for (size_t i = 0; i < parts_to_remove_size; ++i)
        {
            size_t part_size = parts_start_it->size();
            ++parts_start_it;
            bool is_not_last = parts_start_it != parts_end_it;
            full_name_view.remove_prefix(part_size + is_not_last);
        }
    }

    void popFirst()
    {
        popFirst(1);
    }

    void popLast(size_t parts_to_remove_size)
    {
        assert(parts_to_remove_size <= getPartsSize());

        for (size_t i = 0; i < parts_to_remove_size; ++i)
        {
            size_t last_part_size = (parts_end_it - 1)->size();
            --parts_end_it;
            bool is_not_last = parts_start_it != parts_end_it;
            full_name_view.remove_suffix(last_part_size + is_not_last);
        }
    }

    void popLast()
    {
        popLast(1);
    }

    using const_iterator = Identifier::const_iterator;

    const_iterator begin() const
    {
        return parts_start_it;
    }

    const_iterator end() const
    {
        return parts_end_it;
    }
private:
    std::string_view full_name_view;
    const_iterator parts_start_it;
    const_iterator parts_end_it;
};

inline bool operator==(const IdentifierView & lhs, const IdentifierView & rhs)
{
    return lhs.getFullName() == rhs.getFullName();
}

inline bool operator!=(const IdentifierView & lhs, const IdentifierView & rhs)
{
    return !(lhs == rhs);
}

inline std::ostream & operator<<(std::ostream & stream, const IdentifierView & identifier_view)
{
    stream << identifier_view.getFullName();
    return stream;
}

}

template <>
struct std::hash<DB::Identifier>
{
    size_t operator()(const DB::Identifier & identifier) const
    {
        std::hash<std::string> hash;
        return hash(identifier.getFullName());
    }
};

template <>
struct std::hash<DB::IdentifierView>
{
    size_t operator()(const DB::IdentifierView & identifier) const
    {
        std::hash<std::string_view> hash;
        return hash(identifier.getFullName());
    }
};

/// See https://fmt.dev/latest/api.html#formatting-user-defined-types

template <>
struct fmt::formatter<DB::Identifier>
{
    constexpr static auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw fmt::format_error("invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::Identifier & identifier, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", identifier.getFullName());
    }
};

template <>
struct fmt::formatter<DB::IdentifierView>
{
    constexpr static auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw fmt::format_error("invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::IdentifierView & identifier_view, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", identifier_view.getFullName());
    }
};
