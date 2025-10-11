#include <Interpreters/ContextTimeSeriesTagsCollector.h>

#include <Common/Exception.h>
#include <Common/SharedLockGuard.h>
#include <Common/re2.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <boost/container_hash/hash.hpp>
#include <mutex>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    using TagNamesAndValues = ContextTimeSeriesTagsCollector::TagNamesAndValues;
    using TagNamesAndValuesPtr = ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr;
    using Group = ContextTimeSeriesTagsCollector::Group;

    const Group INVALID_GROUP = static_cast<Group>(-1);

    [[noreturn]] void throwGroupOutOfBound(Group group, size_t num_groups)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Group {} is out of bound [0..{}]", group, num_groups - 1);
    }

    template <typename IDType>
    [[noreturn]] void throwIDWasAddedWithOtherTags(const IDType & id, const TagNamesAndValuesPtr & tags, const TagNamesAndValuesPtr & existing_tags)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cannot add identifier {} with tags {} because it was added before with tags {}",
                        toString(id),
                        ContextTimeSeriesTagsCollector::toString(*tags),
                        ContextTimeSeriesTagsCollector::toString(*existing_tags));
    }

    template <typename IDType>
    [[noreturn]] void throwUnknownID(const IDType & id)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown identifier {}", toString(id));
    }

    template <typename TransformFunc2>
    class TransformFunc2To1Adapter
    {
    public:
        TransformFunc2To1Adapter(
            TransformFunc2 && transform_func_, const TagNamesAndValuesPtr & other_argument_, bool is_other_argument_second_)
            : transform_func(std::move(transform_func_))
            , other_argument(other_argument_)
            , is_other_argument_second(is_other_argument_second_)
        {
        }

        TagNamesAndValuesPtr operator()(const TagNamesAndValuesPtr & tags)
        {
            if (is_other_argument_second)
                return transform_func(tags, other_argument);
            else
                return transform_func(other_argument, tags);
        }

    private:
        TransformFunc2 transform_func;
        TagNamesAndValuesPtr other_argument;
        bool is_other_argument_second;
    };

    /// Implements transformation for function removeTagFromGroup().
    class RemoveTagFromGroupTransformFunc
    {
    public:
        explicit RemoveTagFromGroupTransformFunc(const String & tag_to_remove_)
            : tag_to_remove(tag_to_remove_)
        {
        }

        TagNamesAndValuesPtr operator()(const TagNamesAndValuesPtr & old_tags) const
        {
            size_t old_size = old_tags->size();
            size_t mismatch_pos = static_cast<size_t>(-1);

            for (size_t i = 0; i != old_size; ++i)
            {
                const auto & tag_name = (*old_tags)[i].first;
                if (tag_name == tag_to_remove)
                {
                    mismatch_pos = i;
                    break;
                }
            }

            if (mismatch_pos == static_cast<size_t>(-1))
                return old_tags;

            auto new_tags = std::make_shared<TagNamesAndValues>();
            new_tags->reserve(old_size - 1);
            new_tags->assign(old_tags->begin(), old_tags->begin() + mismatch_pos);

            if (mismatch_pos + 1 < old_size)
                new_tags->insert(new_tags->end(), old_tags->begin() + mismatch_pos + 1, old_tags->end());

            return new_tags;
        }

    private:
        std::string_view tag_to_remove;
    };

    /// Implements transformation for function removeTagsFromGroup().
    class RemoveTagsFromGroupTransformFunc
    {
    public:
        explicit RemoveTagsFromGroupTransformFunc(const Strings & tags_to_remove_)
            : tags_to_remove(tags_to_remove_.begin(), tags_to_remove_.end())
        {}

        TagNamesAndValuesPtr operator()(const TagNamesAndValuesPtr & old_tags) const
        {
            size_t old_size = old_tags->size();
            size_t mismatch_pos = static_cast<size_t>(-1);

            for (size_t i = 0; i != old_size; ++i)
            {
                const auto & tag_name = (*old_tags)[i].first;
                if (tags_to_remove.contains(tag_name))
                {
                    mismatch_pos = i;
                    break;
                }
            }

            if (mismatch_pos == static_cast<size_t>(-1))
                return old_tags;

            auto new_tags = std::make_shared<TagNamesAndValues>();
            new_tags->reserve(old_size - 1);
            new_tags->assign(old_tags->begin(), old_tags->begin() + mismatch_pos);

            for (size_t i = mismatch_pos + 1; i != old_size; ++i)
            {
                const auto & tag_name_and_value = (*old_tags)[i];
                if (!tags_to_remove.contains(tag_name_and_value.first))
                    new_tags->emplace_back(tag_name_and_value);
            }

            return new_tags;
        }

    private:
        std::unordered_set<std::string_view> tags_to_remove;
    };

    /// Implements transformation for function removeAllTagsFromGroupExcept().
    class RemoveAllTagsFromGroupExceptTransformFunc
    {
    public:
        explicit RemoveAllTagsFromGroupExceptTransformFunc(const Strings & tags_to_keep_)
            : tags_to_keep(tags_to_keep_.begin(), tags_to_keep_.end())
        {}

        TagNamesAndValuesPtr operator()(const TagNamesAndValuesPtr & old_tags) const
        {
            size_t old_size = old_tags->size();
            size_t mismatch_pos = static_cast<size_t>(-1);

            for (size_t i = 0; i != old_size; ++i)
            {
                const auto & tag_name = (*old_tags)[i].first;
                if (!tags_to_keep.contains(tag_name))
                {
                    mismatch_pos = i;
                    break;
                }
            }

            if (mismatch_pos == static_cast<size_t>(-1))
                return old_tags;

            auto new_tags = std::make_shared<TagNamesAndValues>();
            new_tags->reserve(old_size - 1);
            new_tags->assign(old_tags->begin(), old_tags->begin() + mismatch_pos);

            for (size_t i = mismatch_pos + 1; i != old_size; ++i)
            {
                const auto & tag_name_and_value = (*old_tags)[i];
                if (tags_to_keep.contains(tag_name_and_value.first))
                    new_tags->emplace_back(tag_name_and_value);
            }

            return new_tags;
        }

    private:
        std::unordered_set<std::string_view> tags_to_keep;
    };

    /// Implements transformation for function appendTagsFromGroup().
    class CopyTagsToGroupTransformFunc2
    {
    public:
        explicit CopyTagsToGroupTransformFunc2(const Strings & tags_to_copy_)
        {
            tags_to_copy.reserve(tags_to_copy_.size());
            for (const auto & tag_name : tags_to_copy_)
            {
                bool inserted = positions_in_tags_to_copy.try_emplace(tag_name, tags_to_copy.size()).second;
                if (inserted)
                    tags_to_copy.emplace_back(tag_name, std::string_view{});
            }
            num_tags_to_copy = tags_to_copy.size();
        }

        TagNamesAndValuesPtr operator()(const TagNamesAndValuesPtr & dest_tags, const TagNamesAndValuesPtr & src_tags)
        {
            /// Extract the values of the tags we're going to copy from `src_tags`.
            size_t num_found_tags_in_src = 0;
            for (const auto & [tag_name, tag_value] : *src_tags)
            {
                auto it = positions_in_tags_to_copy.find(tag_name);
                if (it != positions_in_tags_to_copy.end())
                {
                    size_t i = it->second;
                    tags_to_copy[i].second = tag_value;
                    ++num_found_tags_in_src;
                }
            }

            /// Calculate number of tags in the result group.
            size_t dest_size = dest_tags->size();
            size_t new_size = dest_size + num_found_tags_in_src;
            for (const auto & [tag_name, _] : *dest_tags)
            {
                if (positions_in_tags_to_copy.contains(tag_name))
                    --new_size;
            }

            auto new_tags = std::make_shared<TagNamesAndValues>();
            new_tags->reserve(new_size);

            /// Merge two sorted lists `dest_tags` and `tags_to_copy` into one sorted list `new_tags`.
            /// NOTE: Some elements in `tags_to_copy` can have empty values which means we should skip them.
            size_t i = 0;
            size_t j = 0;
            for (;;)
            {
                if (i == dest_size)
                {
                    for (size_t k = i; k != num_tags_to_copy; ++k)
                    {
                        auto & [tag_to_copy, value_to_copy] = tags_to_copy[k];
                        if (!value_to_copy.empty())
                        {
                            new_tags->emplace_back(tag_to_copy, value_to_copy);
                            value_to_copy = {}; /// Clear the values of the copied tags for the next call of operator().
                        }
                    }
                    break;
                }
                if (j == num_tags_to_copy)
                {
                    new_tags->insert(new_tags->end(), dest_tags->begin() + i, dest_tags->end());
                    break;
                }
                const auto & [dest_tag, dest_value] = (*dest_tags)[i];
                auto & [tag_to_copy, value_to_copy] = tags_to_copy[j];
                int cmp = dest_tag.compare(tag_to_copy);
                if (cmp < 0)
                {
                    new_tags->emplace_back(dest_tag, dest_value);
                    ++i;
                }
                else
                {
                    if (!value_to_copy.empty())
                        new_tags->emplace_back(tag_to_copy, value_to_copy);
                    if (cmp == 0)
                        ++i;
                    ++j;
                    value_to_copy = {}; /// Clear the values of the copied tags for the next call of operator().
                }
            }
            chassert(new_tags->size() == new_size);
            return new_tags;
        }

    private:
        std::vector<std::pair<std::string_view, std::string_view>> tags_to_copy;
        std::unordered_map<std::string_view, size_t> positions_in_tags_to_copy;
        size_t num_tags_to_copy;
    };

    /// Adds the tag `dest_tag` with a specified value to the list of tags keeping the list sorted.
    /// If the specified value is empty then the function will remove this tag from the list.
    TagNamesAndValuesPtr addDestTag(const TagNamesAndValuesPtr & old_tags, std::string_view dest_tag, String && dest_value)
    {
        size_t old_size = old_tags->size();
        size_t new_size = old_size;

        if (!dest_value.empty())
            ++new_size;
        
        for (const auto & [tag_name, _] : *old_tags)
        {
            if (tag_name == dest_tag)
                --new_size;
        }

        auto new_tags = std::make_shared<TagNamesAndValues>();
        new_tags->reserve(new_size);

        for (auto it = old_tags->begin(); it != old_tags->end(); ++it)
        {
            const auto & [tag_name, tag_value] = *it;
            int cmp = tag_name.compare(dest_tag);
            if (cmp < 0)
            {
                new_tags->emplace_back(tag_name, tag_value);
            }
            else
            {
                if (!dest_value.empty())
                    new_tags->emplace_back(dest_tag, std::exchange(dest_value, {}));
                if (cmp == 0)
                    ++it;
                new_tags->insert(new_tags->end(), it, old_tags->end());
                break;
            }
        }

        if (!dest_value.empty())
            new_tags->emplace_back(dest_tag, std::move(dest_value));

        chassert(new_tags->size() == new_size);
        return new_tags;
    }

    /// Implements transformation for function labelJoin().
    class LabelJoinTransformFunc
    {
    public:
        LabelJoinTransformFunc(const String & dest_tag_, const String & separator_, const Strings & src_tags_)
        : dest_tag(dest_tag_), separator(separator_)
        {
            src_values.resize(src_tags_.size());
            for (size_t i = 0; i != src_tags_.size(); ++i)
                positions_in_src_tags[src_tags_[i]].push_back(i);
            separators_total_length = src_values.empty() ? 0 : (separator.length() * (src_values.size() - 1));
        }

        TagNamesAndValuesPtr operator()(const TagNamesAndValuesPtr & old_tags)
        {
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

            if (dest_length)
            {
                dest_value.reserve(dest_length);
                chassert(!src_values.empty());
                dest_value += src_values[0];
                src_values[0] = {};
                for (size_t i = 1; i != src_values.size(); ++i)
                {
                    dest_value += separator;
                    dest_value += src_values[i];
                    src_values[i] = {};
                }
            }

            /// Add the tag `dest_tag` to the list of tags.
            return addDestTag(old_tags, dest_tag, std::move(dest_value));
        }

    private:
        std::string_view dest_tag;
        std::string_view separator;
        std::unordered_map<std::string_view, std::vector<size_t>> positions_in_src_tags;
        std::vector<std::string_view> src_values;
        size_t separators_total_length;
    };

    /// Implements transformation for function labelReplace().
    class LabelReplaceTransformFunc
    {
    public:
        LabelReplaceTransformFunc(const String & dest_tag_, const String & replacement_, const String & src_tag_, const String & regex_)
            : dest_tag(dest_tag_)
            , src_tag(src_tag_)
            , regex(regex_)
        {
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
            if (!regex.Match(src_value, 0, src_value.length(), re2::RE2::ANCHOR_BOTH, submatches.data(), submatches.size()))
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
        void parseReplacementPattern(std::string_view replacement_)
        {
            for (size_t pos = 0; pos != replacement_.length();)
            {
                if (replacement_[pos] != '$')
                {
                    size_t next_dollar = replacement_.find('$', pos);
                    if (next_dollar == String::npos)
                        next_dollar = replacement_.length();

                    addTextFragment(replacement_.substr(pos, next_dollar - pos));
                    pos = next_dollar;
                }
                else if (pos + 1 == replacement_.length())
                {
                    addTextFragment(replacement_[pos++]);
                }
                else if (replacement_[pos + 1] == '$')
                {
                    addTextFragment("$");
                    pos += 2;
                }
                else if (std::isdigit(replacement_[pos + 1]))
                {
                    addCapturingGroupFragment(replacement_[pos + 1] - '0');
                    pos += 2;
                }
                else if (std::isalnum(replacement_[pos + 1]) || replacement_[pos + 1] == '_')
                {
                    size_t i = pos + 2;
                    while ((i < replacement_.length()) && (std::isalnum(replacement_[i]) || (replacement_[i] == '_')))
                        ++i;
                    size_t after_name = i;
                    addCapturingGroupFragment(replacement_.substr(pos + 1, after_name - pos - 1));
                    pos = after_name;
                }
                else if (replacement_[pos + 1] != '{')
                {
                    addTextFragment(replacement_[pos++]);
                }
                else if (size_t closing_brace = replacement_.find('}', pos + 2); closing_brace == String::npos)
                {
                    addTextFragment(replacement_[pos++]);
                }
                else
                {
                    std::string_view between_braces = replacement_.substr(pos + 2, closing_brace - pos - 2);
                    if ((between_braces.length() == 1) && std::isdigit(between_braces[0]))
                        addCapturingGroupFragment(between_braces[0] - '0');
                    else
                        addCapturingGroupFragment(between_braces);
                    pos = closing_brace + 1;
                }
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

        void addCapturingGroupFragment(int capturing_group)
        {
            if (capturing_group <= regex.NumberOfCapturingGroups())
                replacement_fragments.emplace_back().capturing_group = capturing_group;
        }

        void addCapturingGroupFragment(std::string_view named_capturing_group)
        {
            const auto & groups = regex.NamedCapturingGroups();
            auto it = groups.find(String{named_capturing_group});
            if (it != groups.end())
                addCapturingGroupFragment(it->second);
        }

        std::string_view dest_tag;
        std::string_view src_tag;
        re2::RE2 regex;

        struct ReplacementFragment
        {
            String text;
            int capturing_group = -1;
        };

        std::vector<ReplacementFragment> replacement_fragments;
        std::vector<std::string_view> submatches;
    };
}


String ContextTimeSeriesTagsCollector::toString(const TagNamesAndValues & tags)
{
    WriteBufferFromOwnString ostr;
    ostr << "{";
    for (size_t i = 0; i != tags.size(); ++i)
    {
        if (i)
            ostr << ", ";
        ostr << quoteString(tags[i].first) << ": " << quoteString(tags[i].second);
    }
    ostr << "}";
    return ostr.str();
}


bool ContextTimeSeriesTagsCollector::Equal::operator()(const TagNamesAndValuesPtr & left, const TagNamesAndValuesPtr & right) const
{
    return *left == *right;
}


size_t ContextTimeSeriesTagsCollector::Hash::operator ()(const TagNamesAndValuesPtr & ptr) const
{
    const auto & tags = *ptr;
    UInt64 hash = 0;
    for (const auto & [tag_name, tag_value] : tags)
    {
        hash = CityHash_v1_0_2::CityHash64WithSeed(tag_name.data(), tag_name.length(), hash);
        hash = CityHash_v1_0_2::CityHash64WithSeed(tag_value.data(), tag_value.length(), hash);
    }
    return hash;
}


ContextTimeSeriesTagsCollector::ContextTimeSeriesTagsCollector()
{
    /// Group #0 is reserved for an empty set of tags.
    auto no_tags = std::make_shared<TagNamesAndValues>();
    groups.push_back(no_tags);
    groups_for_tags.try_emplace(no_tags, 0);
}


ContextTimeSeriesTagsCollector::~ContextTimeSeriesTagsCollector() = default;


Group ContextTimeSeriesTagsCollector::getGroupForTags(const TagNamesAndValuesPtr & tags)
{
    {
        SharedLockGuard lock{mutex};
        auto it = groups_for_tags.find(tags);
        if (it != groups_for_tags.end())
            return it->second;
    }

    {
        std::lock_guard lock{mutex};
        auto [it, inserted] = groups_for_tags.try_emplace(tags, groups.size());
        if (inserted)
            groups.push_back(tags);
        return it->second;
    }
}


std::vector<Group> ContextTimeSeriesTagsCollector::getGroupForTags(const std::vector<TagNamesAndValuesPtr> & tags_vector)
{
    std::vector<Group> res;
    res.resize(tags_vector.size(), INVALID_GROUP);
    size_t num_found = 0;

    {
        SharedLockGuard lock{mutex};
        for (size_t i = 0; i != tags_vector.size(); ++i)
        {
            const auto & tags = tags_vector[i];
            auto it = groups_for_tags.find(tags);
            if (it != groups_for_tags.end())
            {
                res[i] = it->second;
                ++num_found;
            }
        }
    }

    if (num_found != tags_vector.size())
    {
        std::lock_guard lock{mutex};
        for (size_t i = 0; i != tags_vector.size(); ++i)
        {
            if (res[i] != INVALID_GROUP)
                continue;
            const auto & tags = tags_vector[i];
            auto [it, inserted] = groups_for_tags.try_emplace(tags, groups.size());
            if (inserted)
                groups.push_back(tags);
            res[i] = it->second;
            if (++num_found == tags_vector.size())
                break;
        }
    }

    return res;
}


TagNamesAndValuesPtr ContextTimeSeriesTagsCollector::getTagsByGroup(Group group) const
{
    SharedLockGuard lock{mutex};
    if (group >= groups.size())
        throwGroupOutOfBound(group, groups.size());
    return groups[group];
}


std::vector<TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByGroup(const std::vector<Group> & groups_) const
{
    std::vector<TagNamesAndValuesPtr> res;
    res.resize(groups_.size());
    SharedLockGuard lock{mutex};
    for (size_t i = 0; i != groups_.size(); ++i)
    {
        Group group = groups_[i];
        if (group >= groups.size())
            throwGroupOutOfBound(group, groups.size());
        res[i] = groups[group];
    }
    return res;
}


template <typename IDType>
Group ContextTimeSeriesTagsCollector::insert(const IDType & id, const TagNamesAndValuesPtr & tags)
{
    auto & groups_by_id = getIDMap<IDType>().groups_by_id;

    {
        SharedLockGuard lock{mutex};
        auto it = groups_by_id.find(id);
        if (it != groups_by_id.end())
        {
            Group existing_group = it->second;
            if (*tags != *groups.at(existing_group))
                throwIDWasAddedWithOtherTags(id, tags, groups.at(existing_group));
            return existing_group;
        }
    }

    {
        std::lock_guard lock{mutex};
        auto [it, inserted] = groups_for_tags.try_emplace(tags, groups.size());

        if (inserted)
            groups.push_back(tags);

        Group group = it->second;
        auto it2 = groups_by_id.try_emplace(id, group).first;

        if (group != it2->second)
            throwIDWasAddedWithOtherTags(id, tags, groups.at(it2->second));

        return group;
    }
}


template <typename IDType>
std::vector<Group> ContextTimeSeriesTagsCollector::insert(const std::vector<IDType> & ids, const std::vector<TagNamesAndValuesPtr> & tags_vector)
{
    chassert(ids.size() == tags_vector.size());
    auto & groups_by_id = getIDMap<IDType>().groups_by_id;

    std::vector<Group> res;
    res.resize(tags_vector.size(), INVALID_GROUP);
    size_t num_found = 0;

    {
        SharedLockGuard lock{mutex};
        for (size_t i = 0; i != tags_vector.size(); ++i)
        {
            const auto & id = ids[i];
            const auto & tags = tags_vector[i];
            auto it = groups_by_id.find(id);
            if (it != groups_by_id.end())
            {
                Group existing_group = it->second;
                if (*tags != *groups.at(existing_group))
                    throwIDWasAddedWithOtherTags(id, tags, groups.at(existing_group));
                res[i] = existing_group;
                ++num_found;
            }
        }
    }

    if (num_found != tags_vector.size())
    {
        std::unique_lock lock{mutex};
        for (size_t i = 0; i != tags_vector.size(); ++i)
        {
            if (res[i] != INVALID_GROUP)
                continue;
            const auto & id = ids[i];
            const auto & tags = tags_vector[i];

            auto [it, inserted] = groups_for_tags.try_emplace(tags, groups.size());

            if (inserted)
                groups.push_back(tags);

            Group group = it->second;
            auto it2 = groups_by_id.try_emplace(id, group).first;

            if (group != it2->second)
                throwIDWasAddedWithOtherTags(id, tags, groups.at(it2->second));

            res[i] = group;
            if (++num_found == tags_vector.size())
                break;
        }
    }

    return res;
}


template <typename IDType>
Group ContextTimeSeriesTagsCollector::getGroupByID(const IDType & id) const
{
    const auto & groups_by_id = getIDMap<IDType>().groups_by_id;

    SharedLockGuard lock{mutex};

    auto it = groups_by_id.find(id);
    if (it == groups_by_id.end())
        throwUnknownID(id);

    return it->second;
}


template <typename IDType>
std::vector<Group> ContextTimeSeriesTagsCollector::getGroupByID(const std::vector<IDType> & ids) const
{
    const auto & groups_by_id = getIDMap<IDType>().groups_by_id;

    std::vector<Group> res;
    res.reserve(ids.size());

    SharedLockGuard lock{mutex};
    for (const auto & id : ids)
    {
        auto it = groups_by_id.find(id);
        if (it == groups_by_id.end())
            throwUnknownID(id);
        res.push_back(it->second);
    }

    return res;
}


template <typename IDType>
TagNamesAndValuesPtr ContextTimeSeriesTagsCollector::getTagsByID(const IDType & id) const
{
    const auto & groups_by_id = getIDMap<IDType>().groups_by_id;

    SharedLockGuard lock{mutex};

    auto it = groups_by_id.find(id);
    if (it == groups_by_id.end())
        throwUnknownID(id);

    return groups[it->second];
}

template <typename IDType>
std::vector<TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByID(const std::vector<IDType> & ids) const
{
    const auto & groups_by_id = getIDMap<IDType>().groups_by_id;

    std::vector<TagNamesAndValuesPtr> res;
    res.reserve(ids.size());

    SharedLockGuard lock{mutex};
    for (const auto & id : ids)
    {
        auto it = groups_by_id.find(id);
        if (it == groups_by_id.end())
            throwUnknownID(id);
        res.push_back(groups[it->second]);
    }

    return res;
}


template <typename TranformFunc>
Group ContextTimeSeriesTagsCollector::transformTags(Group group, TranformFunc && transform_func)
{
    auto old_tags = getTagsByGroup(group);
    auto new_tags = transform_func(old_tags);
    if (*new_tags == *old_tags)
        return group;
    return getGroupForTags(new_tags);
}


template <typename TranformFunc>
std::vector<Group> ContextTimeSeriesTagsCollector::transformTags(const std::vector<Group> & groups_, TranformFunc && transform_func)
{
    auto tags_vector = getTagsByGroup(groups_);
    chassert(tags_vector.size() == groups_.size());

    std::unordered_map<Group, size_t> indices_in_result_vector;
    size_t num_new_tags = 0;

    for (size_t i = 0; i != groups_.size(); ++i)
    {
        Group group = groups_[i];
        auto it = indices_in_result_vector.find(group);
        if (it == indices_in_result_vector.end())
        {
            const auto & old_tags = tags_vector[i];
            auto new_tags = transform_func(old_tags);
            indices_in_result_vector[group] = num_new_tags;
            tags_vector[num_new_tags++] = new_tags;
        }
    }

    tags_vector.resize(num_new_tags);

    auto new_groups = getGroupForTags(tags_vector);

    std::vector<Group> res;
    res.reserve(groups_.size());

    for (auto old_group : groups_)
    {
        auto new_group = new_groups.at(indices_in_result_vector.at(old_group));
        res.push_back(new_group);
    }

    return res;
}


Group ContextTimeSeriesTagsCollector::removeTagFromGroup(Group group, const String & tag_to_remove)
{
    return transformTags(group, RemoveTagFromGroupTransformFunc{tag_to_remove});
}


std::vector<Group> ContextTimeSeriesTagsCollector::removeTagFromGroup(const std::vector<Group> & groups_, const String & tag_to_remove)
{
    return transformTags(groups_, RemoveTagFromGroupTransformFunc{tag_to_remove});
}


Group ContextTimeSeriesTagsCollector::removeTagsFromGroup(Group group, const Strings & tags_to_remove)
{
    return transformTags(group, RemoveTagsFromGroupTransformFunc{tags_to_remove});
}


std::vector<Group> ContextTimeSeriesTagsCollector::removeTagsFromGroup(const std::vector<Group> & groups_, const Strings & tags_to_remove)
{
    return transformTags(groups_, RemoveTagsFromGroupTransformFunc{tags_to_remove});
}


Group ContextTimeSeriesTagsCollector::removeAllTagsFromGroupExcept(Group group, const Strings & tags_to_keep)
{
    return transformTags(group, RemoveAllTagsFromGroupExceptTransformFunc{tags_to_keep});
}


std::vector<Group> ContextTimeSeriesTagsCollector::removeAllTagsFromGroupExcept(const std::vector<Group> & groups_, const Strings & tags_to_keep)
{
    return transformTags(groups_, RemoveAllTagsFromGroupExceptTransformFunc{tags_to_keep});
}


template <typename TransformFunc2>
Group ContextTimeSeriesTagsCollector::transformTags2(Group group1, Group group2, TransformFunc2 && transform_func)
{
    auto tags1 = getTagsByGroup(group1);
    auto tags2 = getTagsByGroup(group2);
    auto new_tags = transform_func(tags1, tags2);
    return getGroupForTags(new_tags);
}


template <typename TransformFunc2>
std::vector<Group>
ContextTimeSeriesTagsCollector::transformTags2(const std::vector<Group> & groups1, Group group2, TransformFunc2 && transform_func)
{
    return transformTags(
        groups1,
        TransformFunc2To1Adapter<TransformFunc2>{
            std::move(transform_func), /* other_argument = */ getTagsByGroup(group2), /* is_other_argument_second = */ true});
}


template <typename TransformFunc2>
std::vector<Group>
ContextTimeSeriesTagsCollector::transformTags2(Group group1, const std::vector<Group> & groups2, TransformFunc2 && transform_func)
{
    return transformTags(
        groups2,
        TransformFunc2To1Adapter<TransformFunc2>{
            std::move(transform_func), /* other_argument = */ getTagsByGroup(group1), /* is_other_argument_second = */ false});
}


template <typename TransformFunc2>
std::vector<Group> ContextTimeSeriesTagsCollector::transformTags2(const std::vector<Group> & groups1, const std::vector<Group> & groups2, TransformFunc2 && transform_func)
{
    chassert(groups1.size() == groups2.size());

    auto tags_vector1 = getTagsByGroup(groups1);
    auto tags_vector2 = getTagsByGroup(groups2);
    chassert(tags_vector1.size() == groups1.size());
    chassert(tags_vector2.size() == groups2.size());

    std::unordered_map<std::pair<Group, Group>, size_t, boost::hash<std::pair<Group, Group>>> indices_in_result_vector;
    size_t num_new_tags = 0;

    for (size_t i = 0; i != groups1.size(); ++i)
    {
        Group group1 = groups1[i];
        Group group2 = groups2[i];
        auto it = indices_in_result_vector.find(std::make_pair(group1, group2));
        if (it == indices_in_result_vector.end())
        {
            const auto & tags1 = tags_vector1[i];
            const auto & tags2 = tags_vector2[i];
            auto new_tags = transform_func(tags1, tags2);
            indices_in_result_vector[std::make_pair(group1, group2)] = num_new_tags;
            tags_vector1[num_new_tags++] = new_tags;
        }
    }

    tags_vector1.resize(num_new_tags);

    auto new_groups = getGroupForTags(tags_vector1);

    std::vector<Group> res;
    res.reserve(groups1.size());

    for (size_t i = 0; i != groups1.size(); ++i)
    {
        Group group1 = groups1[i];
        Group group2 = groups2[i];
        auto new_group = new_groups.at(indices_in_result_vector.at(std::make_pair(group1, group2)));
        res.push_back(new_group);
    }

    return res;
}


Group ContextTimeSeriesTagsCollector::copyTagsToGroup(Group dest_group, Group src_group, const Strings & tags_to_copy)
{
    return transformTags2(dest_group, src_group, CopyTagsToGroupTransformFunc2{tags_to_copy});
}


std::vector<Group> ContextTimeSeriesTagsCollector::copyTagsToGroup(Group dest_group, const std::vector<Group> & src_groups, const Strings & tags_to_copy)
{
    return transformTags2(dest_group, src_groups, CopyTagsToGroupTransformFunc2{tags_to_copy});
}


std::vector<Group> ContextTimeSeriesTagsCollector::copyTagsToGroup(const std::vector<Group> & dest_groups, Group src_group, const Strings & tags_to_copy)
{
    return transformTags2(dest_groups, src_group, CopyTagsToGroupTransformFunc2{tags_to_copy});
}


std::vector<Group> ContextTimeSeriesTagsCollector::copyTagsToGroup(const std::vector<Group> & dest_groups, const std::vector<Group> & src_groups, const Strings & tags_to_copy)
{
    return transformTags2(dest_groups, src_groups, CopyTagsToGroupTransformFunc2{tags_to_copy});
}


Group ContextTimeSeriesTagsCollector::labelJoin(Group group, const String & dest_tag, const String & separator, const Strings & src_tags)
{
    return transformTags(group, LabelJoinTransformFunc{dest_tag, separator, src_tags});
}


std::vector<Group> ContextTimeSeriesTagsCollector::labelJoin(const std::vector<Group> & groups_, const String & dest_tag, const String & separator, const Strings & src_tags)
{
    return transformTags(groups_, LabelJoinTransformFunc{dest_tag, separator, src_tags});
}


Group ContextTimeSeriesTagsCollector::labelReplace(Group group, const String & dest_tag, const String & replacement, const String & src_tag, const String & regex)
{
    return transformTags(group, LabelReplaceTransformFunc{dest_tag, replacement, src_tag, regex});
}


std::vector<Group> ContextTimeSeriesTagsCollector::labelReplace(const std::vector<Group> & groups_, const String & dest_tag, const String & replacement, const String & src_tag, const String & regex)
{
    return transformTags(groups_, LabelReplaceTransformFunc{dest_tag, replacement, src_tag, regex});
}


template <typename IDType>
ContextTimeSeriesTagsCollector::IDMap<IDType> & ContextTimeSeriesTagsCollector::getIDMap()
{
    if constexpr (std::is_same_v<IDType, UInt64>)
    {
        return uint64_id_map;
    }
    else
    {
        static_assert(std::is_same_v<IDType, UInt128>);
        return uint128_id_map;
    }
}

template <typename IDType>
const ContextTimeSeriesTagsCollector::IDMap<IDType> & ContextTimeSeriesTagsCollector::getIDMap() const
{
    return const_cast<ContextTimeSeriesTagsCollector *>(this)->getIDMap<IDType>();
}


#define TIME_SERIES_ID_TO_TAGS_MAP_INSTANTIATE(IDType) \
    template Group ContextTimeSeriesTagsCollector::insert<IDType>(const IDType & id, const TagNamesAndValuesPtr & tags); \
    template std::vector<Group> ContextTimeSeriesTagsCollector::insert<IDType>(const std::vector<IDType> & ids, const std::vector<TagNamesAndValuesPtr> & tags_vector); \
    template Group ContextTimeSeriesTagsCollector::getGroupByID<IDType>(const IDType & id) const; \
    template std::vector<Group> ContextTimeSeriesTagsCollector::getGroupByID<IDType>(const std::vector<IDType> & ids) const; \
    template ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr ContextTimeSeriesTagsCollector::getTagsByID<IDType>(const IDType & id) const; \
    template std::vector<ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByID<IDType>(const std::vector<IDType> & ids) const; \

TIME_SERIES_ID_TO_TAGS_MAP_INSTANTIATE(UInt64)
TIME_SERIES_ID_TO_TAGS_MAP_INSTANTIATE(UInt128)

#undef TIME_SERIES_ID_TO_TAGS_MAP_INSTANTIATE

}
