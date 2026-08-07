#include <Analyzer/Identifier.h>
#include <base/defines.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>


namespace DB
{

/// Create Identifier from parts
Identifier::Identifier(const std::vector<std::string> & parts_)
    : parts(parts_)
    , full_name(boost::algorithm::join(parts, "."))
{
}

/// Create Identifier from parts
Identifier::Identifier(std::vector<std::string> && parts_)
    : parts(std::move(parts_))
    , full_name(boost::algorithm::join(parts, "."))
{
}

/// Create Identifier from full name, full name is split with '.' as separator.
Identifier::Identifier(const std::string & full_name_)
    : full_name(full_name_)
{
    boost::split(parts, full_name, [](char c) { return c == '.'; });
}

/// Create Identifier from full name, full name is split with '.' as separator.
Identifier::Identifier(std::string && full_name_) : full_name(std::move(full_name_))
{
    boost::split(parts, full_name, [](char c) { return c == '.'; });
}

void Identifier::popFirst(size_t parts_to_remove_size)
{
    chassert(parts_to_remove_size <= parts.size());

    size_t parts_size = parts.size();
    std::vector<std::string> result_parts;
    result_parts.reserve(parts_size - parts_to_remove_size);

    for (size_t i = parts_to_remove_size; i < parts_size; ++i)
        result_parts.push_back(std::move(parts[i]));

    parts = std::move(result_parts);
    full_name = boost::algorithm::join(parts, ".");
}

void Identifier::popLast(size_t parts_to_remove_size)
{
    chassert(parts_to_remove_size <= parts.size());

    for (size_t i = 0; i < parts_to_remove_size; ++i)
    {
        size_t last_part_size = parts.back().size();
        parts.pop_back();
        bool is_not_last = !parts.empty();
        full_name.resize(full_name.size() - (last_part_size + static_cast<size_t>(is_not_last)));
    }
}

void IdentifierView::popFirst(size_t parts_to_remove_size)
{
    chassert(parts_to_remove_size <= getPartsSize());

    for (size_t i = 0; i < parts_to_remove_size; ++i)
    {
        size_t part_size = parts_start_it->size();
        ++parts_start_it;
        bool is_not_last = parts_start_it != parts_end_it;
        full_name_view.remove_prefix(part_size + is_not_last);
    }
}

void IdentifierView::popLast(size_t parts_to_remove_size)
{
    chassert(parts_to_remove_size <= getPartsSize());

    for (size_t i = 0; i < parts_to_remove_size; ++i)
    {
        size_t last_part_size = (parts_end_it - 1)->size();
        --parts_end_it;
        bool is_not_last = parts_start_it != parts_end_it;
        full_name_view.remove_suffix(last_part_size + is_not_last);
    }
}

}
