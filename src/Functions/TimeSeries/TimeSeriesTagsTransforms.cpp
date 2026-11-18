#include <Functions/TimeSeries/TimeSeriesTagsTransforms.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/re2.h>
#include <IO/ReadHelpers.h>

#include <algorithm>
#include <cctype>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


namespace DB::TimeSeriesTagsTransforms
{

namespace
{
    /// Implements transformation for function removeTag().
    class RemoveTagTransformFunc
    {
    public:
        explicit RemoveTagTransformFunc(const String & tag_to_remove_)
            : tag_to_remove(tag_to_remove_)
        {
        }

        TagNamesAndValuesPtr operator()(const TagNamesAndValuesPtr & old_tags) const
        {
            size_t old_size = old_tags->size();
            size_t remove_pos = static_cast<size_t>(-1);

            for (size_t i = 0; i != old_size; ++i)
            {
                const auto & tag_name = (*old_tags)[i].first;
                if (tag_name == tag_to_remove)
                {
                    remove_pos = i;
                    break;
                }
            }

            if (remove_pos == static_cast<size_t>(-1))
                return old_tags;

            auto new_tags = std::make_shared<TagNamesAndValues>();
            new_tags->reserve(old_size - 1);

            new_tags->assign(old_tags->begin(), old_tags->begin() + remove_pos);

            if (remove_pos + 1 < old_size)
                new_tags->insert(new_tags->end(), old_tags->begin() + remove_pos + 1, old_tags->end());

            return new_tags;
        }

    private:
        std::string_view tag_to_remove;
    };

    /// Implements transformation for function removeTags().
    class RemoveTagsTransformFunc
    {
    public:
        explicit RemoveTagsTransformFunc(const Strings & tags_to_remove_)
            : tags_to_remove(tags_to_remove_.begin(), tags_to_remove_.end())
        {}

        TagNamesAndValuesPtr operator()(const TagNamesAndValuesPtr & old_tags) const
        {
            size_t old_size = old_tags->size();
            size_t remove_pos = static_cast<size_t>(-1);

            for (size_t i = 0; i != old_size; ++i)
            {
                const auto & tag_name = (*old_tags)[i].first;
                if (tags_to_remove.contains(tag_name))
                {
                    remove_pos = i;
                    break;
                }
            }

            if (remove_pos == static_cast<size_t>(-1))
                return old_tags;

            auto new_tags = std::make_shared<TagNamesAndValues>();
            new_tags->reserve(old_size - 1);
            new_tags->assign(old_tags->begin(), old_tags->begin() + remove_pos);

            for (size_t i = remove_pos + 1; i != old_size; ++i)
            {
                const auto & tag_name_and_value = (*old_tags)[i];
                if (!tags_to_remove.contains(tag_name_and_value.first))
                    new_tags->emplace_back(tag_name_and_value);
            }

            return new_tags;
        }

    private:
        UnorderedSetWithMemoryTracking<std::string_view> tags_to_remove;
    };

    /// Implements transformation for function removeAllTagsExcept().
    class RemoveAllTagsExceptTransformFunc
    {
    public:
        explicit RemoveAllTagsExceptTransformFunc(const Strings & tags_to_keep_)
            : tags_to_keep(tags_to_keep_.begin(), tags_to_keep_.end())
        {}

        TagNamesAndValuesPtr operator()(const TagNamesAndValuesPtr & old_tags) const
        {
            size_t old_size = old_tags->size();
            size_t remove_pos = static_cast<size_t>(-1);

            for (size_t i = 0; i != old_size; ++i)
            {
                const auto & tag_name = (*old_tags)[i].first;
                if (!tags_to_keep.contains(tag_name))
                {
                    remove_pos = i;
                    break;
                }
            }

            if (remove_pos == static_cast<size_t>(-1))
                return old_tags;

            auto new_tags = std::make_shared<TagNamesAndValues>();
            new_tags->reserve(old_size - 1);
            new_tags->assign(old_tags->begin(), old_tags->begin() + remove_pos);

            for (size_t i = remove_pos + 1; i != old_size; ++i)
            {
                const auto & tag_name_and_value = (*old_tags)[i];
                if (tags_to_keep.contains(tag_name_and_value.first))
                    new_tags->emplace_back(tag_name_and_value);
            }

            return new_tags;
        }

    private:
        UnorderedSetWithMemoryTracking<std::string_view> tags_to_keep;
    };

    /// Implements transformation for function copyTag().
    class CopyTagTransformFunc2
    {
    public:
        explicit CopyTagTransformFunc2(const String & tag_to_copy_)
            : tag_to_copy(tag_to_copy_)
        {
        }

        TagNamesAndValuesPtr operator()(const TagNamesAndValuesPtr & dest_tags, const TagNamesAndValuesPtr & src_tags)
        {
            /// Extract the value of the tag we're going to copy from `src_tags`.
            std::string_view new_value;
            for (const auto & [tag_name, tag_value] : *src_tags)
            {
                if (tag_name == tag_to_copy)
                {
                    new_value = tag_value;
                    break;
                }
            }

            /// Finds the insert position of this tag in `dest_tags`.
            size_t insert_pos = dest_tags->size();
            std::string_view current_value;

            for (size_t i = 0; i != dest_tags->size(); ++i)
            {
                int cmp = (*dest_tags)[i].first.compare(tag_to_copy);
                if (cmp == 0)
                {
                    current_value = (*dest_tags)[i].second;
                    insert_pos = i;
                    break;
                }
                else if (cmp > 0)
                {
                    insert_pos = i;
                    break;
                }
            }

            if (current_value == new_value)
                return dest_tags; /// No need to copy.

            /// Calculate number of tags in the result group.
            size_t new_size = dest_tags->size() + !new_value.empty() - !current_value.empty();

            auto new_tags = std::make_shared<TagNamesAndValues>();
            new_tags->reserve(new_size);

            /// Copy all the tags before `tag_to_copy`.
            new_tags->assign(dest_tags->begin(), dest_tags->begin() + insert_pos);

            if (!new_value.empty())
                new_tags->emplace_back(tag_to_copy, new_value);

            /// Copy all the tags after `tag_to_copy`.
            size_t next_pos = !current_value.empty() ? (insert_pos + 1) : insert_pos;
            new_tags->insert(new_tags->end(), dest_tags->begin() + next_pos, dest_tags->end());

            chassert(new_tags->size() == new_size);
            return new_tags;
        }

    private:
        std::string_view tag_to_copy;
    };

    /// Implements transformation for function copyTags().
    class CopyTagsTransformFunc2
    {
    public:
        explicit CopyTagsTransformFunc2(const Strings & tags_to_copy_)
        {
            tags_to_copy.reserve(tags_to_copy_.size());
            for (const auto & tag_name : tags_to_copy_)
                tags_to_copy.emplace_back(tag_name, std::string_view{});

            /// We make the list `tags_to_copy` sorted because we'll use the merge algorithm in operator().
            std::sort(tags_to_copy.begin(), tags_to_copy.end());
            tags_to_copy.erase(std::unique(tags_to_copy.begin(), tags_to_copy.end()), tags_to_copy.end());

            for (size_t i = 0; i != tags_to_copy.size(); ++i)
                positions_in_tags_to_copy[tags_to_copy[i].first] = i;

            num_tags_to_copy = tags_to_copy.size();
        }

        TagNamesAndValuesPtr operator()(const TagNamesAndValuesPtr & dest_tags, const TagNamesAndValuesPtr & src_tags)
        {
            /// Clear the values which were copied last time operator() was called.
            for (auto & [_, new_value] : tags_to_copy)
                new_value = {};

            /// Extract the values of the tags we're going to copy from `src_tags`.
            size_t num_new_values = 0;
            for (const auto & [tag_name, tag_value] : *src_tags)
            {
                auto it = positions_in_tags_to_copy.find(tag_name);
                if (it != positions_in_tags_to_copy.end())
                {
                    size_t j = it->second;
                    auto & new_value = tags_to_copy[j].second;
                    if (!tag_value.empty() && new_value.empty())
                    {
                        new_value = tag_value;
                        ++num_new_values;
                    }
                }
            }

            size_t num_dest_tags = dest_tags->size();

            auto new_tags = std::make_shared<TagNamesAndValues>();
            new_tags->reserve(num_dest_tags + num_new_values);

            /// Merge two sorted lists `dest_tags` and `tags_to_copy` into one sorted list `new_tags`.
            /// NOTE: Some elements in `tags_to_copy` may have empty values which means we should skip them.

            size_t i = 0; /// index in `dest_tags`
            size_t j = 0; /// index in `tags_to_copy`
            while ((i < num_dest_tags) && (j < num_tags_to_copy))
            {
                const auto & [dest_tag, dest_value] = (*dest_tags)[i];
                const auto & [tag_to_copy, new_value] = tags_to_copy[j];
                int cmp = dest_tag.compare(tag_to_copy);
                if (cmp < 0)
                {
                    new_tags->emplace_back(dest_tag, dest_value);
                    ++i;
                }
                else
                {
                    if (!new_value.empty())
                        new_tags->emplace_back(tag_to_copy, new_value);
                    if (cmp == 0)
                        ++i;
                    ++j;
                }
            }

            if (i < num_dest_tags)
            {
                new_tags->insert(new_tags->end(), dest_tags->begin() + i, dest_tags->end());
            }

            if (j < num_tags_to_copy)
            {
                for (; j != num_tags_to_copy; ++j)
                {
                    auto & [tag_to_copy, new_value] = tags_to_copy[j];
                    if (!new_value.empty())
                        new_tags->emplace_back(tag_to_copy, new_value);
                }
            }

            return new_tags;
        }

    private:
        VectorWithMemoryTracking<std::pair<std::string_view, std::string_view>> tags_to_copy;
        UnorderedMapWithMemoryTracking<std::string_view, size_t> positions_in_tags_to_copy;
        size_t num_tags_to_copy;
    };

    /// Adds the tag `dest_tag` with a specified value to the list of tags keeping the list sorted.
    /// If the specified value is empty then the function will remove this tag from the list.
    TagNamesAndValuesPtr addDestTag(const TagNamesAndValuesPtr & old_tags, std::string_view dest_tag, String && dest_value)
    {
        size_t insert_pos = old_tags->size();
        std::string_view current_value;

        for (size_t i = 0; i != old_tags->size(); ++i)
        {
            const auto & [tag_name, tag_value] = (*old_tags)[i];
            int cmp = tag_name.compare(dest_tag);
            if (cmp == 0)
            {
                current_value = tag_value;
                insert_pos = i;
                break;
            }
            else if (cmp > 0)
            {
                insert_pos = i;
                break;
            }
        }

        if (current_value == dest_value)
            return old_tags;

        /// Calculate number of tags in the result group.
        size_t new_size = old_tags->size() + !dest_value.empty() - !current_value.empty();

        auto new_tags = std::make_shared<TagNamesAndValues>();
        new_tags->reserve(new_size);

        /// Copy all the tags before `dest_tag`.
        new_tags->assign(old_tags->begin(), old_tags->begin() + insert_pos);

        if (!dest_value.empty())
            new_tags->emplace_back(dest_tag, std::move(dest_value));

        /// Copy all the tags after `dest_tag`.
        size_t next_pos = !current_value.empty() ? (insert_pos + 1) : insert_pos;
        new_tags->insert(new_tags->end(), old_tags->begin() + next_pos, old_tags->end());

        chassert(new_tags->size() == new_size);
        return new_tags;
    }

    /// Implements transformation for function joinTags().
    class JoinTagsTransformFunc
    {
    public:
        JoinTagsTransformFunc(const String & dest_tag_, const String & separator_, const Strings & src_tags_)
        : dest_tag(dest_tag_), separator(separator_)
        {
            src_values.resize(src_tags_.size());
            for (size_t i = 0; i != src_tags_.size(); ++i)
                positions_in_src_tags[src_tags_[i]].push_back(i);
            separators_total_length = src_values.empty() ? 0 : (separator.length() * (src_values.size() - 1));
        }

        TagNamesAndValuesPtr operator()(const TagNamesAndValuesPtr & old_tags)
        {
            /// Clear the values which were copied last time operator() was called.
            for (auto & src_value : src_values)
                src_value = {};

            size_t dest_length = separators_total_length;

            /// Collect all values we're going to concatenate in `src_values` in the correct order.
            for (const auto & [tag_name, tag_value] : *old_tags)
            {
                auto it = positions_in_src_tags.find(tag_name);
                if (it != positions_in_src_tags.end())
                {
                    for (size_t i : it->second)
                    {
                        src_values[i] = tag_value;
                        dest_length += tag_value.length();
                    }
                }
            }

            /// Calculate the concatenated value.
            String dest_value;

            if (!src_values.empty())
            {
                dest_value.reserve(dest_length);
                dest_value += src_values[0];
                for (size_t i = 1; i != src_values.size(); ++i)
                {
                    dest_value += separator;
                    dest_value += src_values[i];
                }
            }

            /// Add the tag `dest_tag` to the list of tags.
            return addDestTag(old_tags, dest_tag, std::move(dest_value));
        }

    private:
        std::string_view dest_tag;
        std::string_view separator;
        UnorderedMapWithMemoryTracking<std::string_view, VectorWithMemoryTracking<size_t>> positions_in_src_tags;
        VectorWithMemoryTracking<std::string_view> src_values;
        size_t separators_total_length;
    };

    /// Implements transformation for function replaceTag().
    class ReplaceTagTransformFunc
    {
    public:
        ReplaceTagTransformFunc(const String & dest_tag_, const String & replacement_, const String & src_tag_, const String & regex_)
            : dest_tag(dest_tag_)
            , src_tag(src_tag_)
            , regex(regex_)
        {
            if (!regex.ok())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Invalid regular expression {}: {}",
                                quoteString(regex_), regex.error());
            }
            parseReplacementPattern(replacement_);
            submatches.resize(1 + regex.NumberOfCapturingGroups());
        }

        TagNamesAndValuesPtr operator()(const TagNamesAndValuesPtr & old_tags)
        {
            /// Find `src_tag` in the old tags.
            std::string_view src_value;
            for (const auto & [tag_name, tag_value] : *old_tags)
            {
                if (tag_name == src_tag)
                    src_value = tag_value;
            }

            /// Check if it matches and extract submatches if it is so.
            if (!regex.Match(src_value, 0, src_value.length(), re2::RE2::ANCHOR_BOTH, submatches.data(), static_cast<int>(submatches.size())))
            {
                /// If the regular expression doesn't match then the original tags are returned unchanged.
                return old_tags;
            }

            /// Calculate the replacement using the specified pattern and extracted submatches.
            String dest_value;
            for (const auto & fragment : replacement_fragments)
            {
                if (!fragment.text.empty())
                    dest_value += fragment.text;
                else
                    dest_value += submatches.at(fragment.capturing_group);
            }

            /// Add the tag `dest_tag` to the list of tags.
            return addDestTag(old_tags, dest_tag, std::move(dest_value));
        }

    private:
        /// Parses a replacement pattern using the same rules as Prometheus label_replace() function:
        /// - `$$` is a literal `$`.
        /// - `$name` and `${name}` reference a capture group, where `name` is the longest
        ///   run of letters, digits, and underscores after `$` (or until `}` for the braced form).
        /// - If `name` is purely decimal digits AND not a multi-digit string with a leading zero
        ///   (so "0", "1", "11" qualify but "01" does not), it's a numeric group reference;
        ///   otherwise it's a named-group reference.
        /// - `$` followed by invalid name characters or an unmatched `${...` is treated as literal text.
        void parseReplacementPattern(std::string_view replacement_)
        {
            for (size_t pos = 0; pos != replacement_.length();)
            {
                if (replacement_[pos] != '$')
                {
                    addTextFragment(replacement_[pos]);
                    ++pos;
                    continue;
                }

                if ((pos + 1 < replacement_.length()) && (replacement_[pos + 1] == '$'))
                {
                    addTextFragment('$');
                    pos += 2;
                    continue;
                }

                bool brace = (pos + 1 < replacement_.length()) && (replacement_[pos + 1] == '{');
                size_t name_start = pos + 1 + (brace ? 1 : 0);
                size_t name_end = name_start;
                while ((name_end < replacement_.length())
                       && (std::isalnum(static_cast<unsigned char>(replacement_[name_end])) || replacement_[name_end] == '_'))
                    ++name_end;

                /// `$` with no name, or `${...` without a closing `}`: treat the `$` as literal.
                if ((name_end == name_start) || (brace && (name_end >= replacement_.length() || replacement_[name_end] != '}')))
                {
                    addTextFragment('$');
                    ++pos;
                    continue;
                }

                addCapturingGroupFragment(replacement_.substr(name_start, name_end - name_start));
                pos = name_end + (brace ? 1 : 0);
            }
        }

        void addTextFragment(std::string_view text)
        {
            if (text.empty())
                return;
            if (replacement_fragments.empty() || replacement_fragments.back().text.empty())
                replacement_fragments.emplace_back(ReplacementFragment{.text = String{text}});
            else
                replacement_fragments.back().text += text;
        }

        void addTextFragment(char c)
        {
            addTextFragment(std::string_view{&c, 1});
        }

        /// Adds a fragment for a `$name` / `${name}` reference.
        /// A name made entirely of decimal digits with no leading zero (single "0" is fine) is treated
        /// as a numeric group, otherwise it's a named-group.
        void addCapturingGroupFragment(std::string_view name)
        {
            chassert(!name.empty());

            /// At most 9 digits: longer all-digit names fall through to a named-group lookup.
            bool numeric = (name.size() <= 9)
                && std::all_of(name.begin(), name.end(), [](char c) { return std::isdigit(static_cast<unsigned char>(c)); });

            /// A multi-digit name with a leading zero (e.g. "01") is treated as a named-group reference,
            /// and not a numeric one.
            if (numeric && (name.size() > 1) && (name.front() == '0'))
                numeric = false;

            if (numeric)
            {
                addCapturingGroupFragment(parseFromString<int>(name));
                return;
            }

            const auto & groups = regex.NamedCapturingGroups();
            auto it = groups.find(String{name});
            if (it != groups.end())
                addCapturingGroupFragment(it->second);
        }

        void addCapturingGroupFragment(int capturing_group)
        {
            if (capturing_group <= regex.NumberOfCapturingGroups())
                replacement_fragments.emplace_back().capturing_group = capturing_group;
        }

        std::string_view dest_tag;
        std::string_view src_tag;
        re2::RE2 regex;

        struct ReplacementFragment
        {
            String text;
            int capturing_group = -1;
        };

        VectorWithMemoryTracking<ReplacementFragment> replacement_fragments;
        VectorWithMemoryTracking<std::string_view> submatches;
    };
}


void removeTag(VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector, const String & tag_to_remove)
{
    RemoveTagTransformFunc transform_func{tag_to_remove};
    for (auto & tags : tags_vector)
        tags = transform_func(tags);
}


void removeTags(VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector, const Strings & tags_to_remove)
{
    RemoveTagsTransformFunc transform_func{tags_to_remove};
    for (auto & tags : tags_vector)
        tags = transform_func(tags);
}


void removeAllTagsExcept(VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector, const Strings & tags_to_keep)
{
    RemoveAllTagsExceptTransformFunc transform_func{tags_to_keep};
    for (auto & tags : tags_vector)
        tags = transform_func(tags);
}


void copyTag(
    VectorWithMemoryTracking<TagNamesAndValuesPtr> & dest_tags_vector,
    const VectorWithMemoryTracking<TagNamesAndValuesPtr> & src_tags_vector,
    const String & tag_to_copy)
{
    chassert(dest_tags_vector.size() == src_tags_vector.size());
    CopyTagTransformFunc2 transform_func{tag_to_copy};
    for (size_t i = 0; i != dest_tags_vector.size(); ++i)
        dest_tags_vector[i] = transform_func(dest_tags_vector[i], src_tags_vector[i]);
}


void copyTags(
    VectorWithMemoryTracking<TagNamesAndValuesPtr> & dest_tags_vector,
    const VectorWithMemoryTracking<TagNamesAndValuesPtr> & src_tags_vector,
    const Strings & tags_to_copy)
{
    chassert(dest_tags_vector.size() == src_tags_vector.size());
    CopyTagsTransformFunc2 transform_func{tags_to_copy};
    for (size_t i = 0; i != dest_tags_vector.size(); ++i)
        dest_tags_vector[i] = transform_func(dest_tags_vector[i], src_tags_vector[i]);
}


void joinTags(
    VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector,
    const String & dest_tag,
    const String & separator,
    const Strings & src_tags)
{
    JoinTagsTransformFunc transform_func{dest_tag, separator, src_tags};
    for (auto & tags : tags_vector)
        tags = transform_func(tags);
}


void replaceTag(
    VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector,
    const String & dest_tag,
    const String & replacement,
    const String & src_tag,
    const String & regex)
{
    ReplaceTagTransformFunc transform_func{dest_tag, replacement, src_tag, regex};
    for (auto & tags : tags_vector)
        tags = transform_func(tags);
}

}
