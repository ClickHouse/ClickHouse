#include <Interpreters/ContextTimeSeriesTagsCollector.h>

#include <Common/PODArray.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitors.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/SharedLockGuard.h>
#include <Common/re2.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <base/unaligned.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <algorithm>
#include <utility>
#include <boost/container_hash/hash.hpp>
#include <city.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

namespace
{
    using TagNamesAndValues = ContextTimeSeriesTagsCollector::TagNamesAndValues;
    using TagNamesAndValuesPtr = ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr;
    using Group = ContextTimeSeriesTagsCollector::Group;

    const Group INVALID_GROUP = static_cast<Group>(-1);

    [[noreturn]] void throwGroupOutOfBound(Group group, size_t num_groups)
    {
        if (num_groups > 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Group {} is out of bounds, must be between 0 and {}", group, num_groups - 1);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No groups exist");
    }

    [[noreturn]] void throwIDWasAddedWithOtherTags(const IColumn & id_column, size_t row, const TagNamesAndValuesPtr & tags, const TagNamesAndValuesPtr & existing_tags)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cannot add identifier {} with tags {} because it was added before with tags {}",
                        applyVisitor(FieldVisitorToString{}, id_column[row]),
                        ContextTimeSeriesTagsCollector::toString(*tags),
                        ContextTimeSeriesTagsCollector::toString(*existing_tags));
    }

    [[noreturn]] void throwUnknownID(const IColumn & id_column, size_t row)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown identifier {}", applyVisitor(FieldVisitorToString{}, id_column[row]));
    }

    /// Represents an identifier column with Const/Sparse/LowCardinality/Nullable wrappers removed.
    struct UnwrappedIDColumn
    {
        ColumnPtr column;                   /// The full column, it keeps `data` and `null_map` alive.
        const IColumn * data = nullptr;     /// The column containing values of identifiers (not Nullable).
        const NullMap * null_map = nullptr; /// The null map if the original column was Nullable.
    };

    UnwrappedIDColumn unwrapIDColumn(const ColumnPtr & id_column)
    {
        UnwrappedIDColumn res;
        res.column = id_column->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();
        res.data = res.column.get();
        res.null_map = nullptr;
        if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(res.data))
        {
            res.null_map = &nullable_column->getNullMapData();
            res.data = &nullable_column->getNestedColumn();
        }
        return res;
    }

    /// Converts an element of an identifier column to its representation in a typed map.
    UInt64 toIDComponent(UInt64 x) { return x; }
    UInt128 toIDComponent(const UInt128 & x) { return x; }
    UInt128 toIDComponent(const UUID & x) { return x.toUnderType(); }

    /// Gets identifiers stored in a single column.
    template <typename Type>
    struct OneComponentID
    {
        using IDType = decltype(toIDComponent(Type{}));
        const Type * data;
        IDType get(size_t i) const { return toIDComponent(data[i]); }
    };

    /// Gets identifiers stored in a `FixedString(16)` column, which holds them as raw bytes
    /// without a separate element type, so they are read with the same representation as UInt128.
    struct FixedString16ID
    {
        using IDType = UInt128;
        const UInt8 * data;
        IDType get(size_t i) const { return unalignedLoad<UInt128>(data + sizeof(UInt128) * i); }
    };

    /// Gets identifiers stored in a two-element tuple column by combining two component getters.
    template <typename FirstGetter, typename SecondGetter>
    struct TwoComponentID
    {
        using IDType = std::pair<typename FirstGetter::IDType, typename SecondGetter::IDType>;
        FirstGetter first;
        SecondGetter second;
        IDType get(size_t i) const { return {first.get(i), second.get(i)}; }
    };

    /// Gets identifier components stored in a LowCardinality column: the per-row dictionary index
    /// addresses the getter of the dictionary column, producing the same native representation
    /// as a plain column of the dictionary type.
    template <typename IndexType, typename DictionaryGetter>
    struct LowCardinalityID
    {
        using IDType = typename DictionaryGetter::IDType;
        const IndexType * indexes;
        DictionaryGetter dictionary;
        IDType get(size_t i) const { return dictionary.get(indexes[i]); }
    };

    /// Calls `func` with the raw data of a LowCardinality index column.
    template <typename F>
    void dispatchLowCardinalityIndexes(const IColumn & indexes, F && func)
    {
        if (const auto * indexes_uint8 = typeid_cast<const ColumnUInt8 *>(&indexes))
            func(indexes_uint8->getData().data());
        else if (const auto * indexes_uint16 = typeid_cast<const ColumnUInt16 *>(&indexes))
            func(indexes_uint16->getData().data());
        else if (const auto * indexes_uint32 = typeid_cast<const ColumnUInt32 *>(&indexes))
            func(indexes_uint32->getData().data());
        else if (const auto * indexes_uint64 = typeid_cast<const ColumnUInt64 *>(&indexes))
            func(indexes_uint64->getData().data());
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected index column {} of a LowCardinality column", indexes.getName());
    }

    /// Calls `func` with the typed id getter matching the column's layout and returns true,
    /// or returns false if there is no typed map for this column's layout (the generic map must be used).
    template <typename F>
    bool dispatchIDType(const IColumn & id_data, F && func)
    {
        if (const auto * column_uint64 = typeid_cast<const ColumnUInt64 *>(&id_data))
        {
            func(OneComponentID<UInt64>{column_uint64->getData().data()});
            return true;
        }
        if (const auto * column_uint128 = typeid_cast<const ColumnUInt128 *>(&id_data))
        {
            func(OneComponentID<UInt128>{column_uint128->getData().data()});
            return true;
        }
        if (const auto * column_uuid = typeid_cast<const ColumnUUID *>(&id_data))
        {
            func(OneComponentID<UUID>{column_uuid->getData().data()});
            return true;
        }
        if (const auto * column_fixed_string = typeid_cast<const ColumnFixedString *>(&id_data))
        {
            if (column_fixed_string->getN() == sizeof(UInt128))
            {
                func(FixedString16ID{column_fixed_string->getChars().data()});
                return true;
            }
            return false;
        }

        const auto * column_tuple = typeid_cast<const ColumnTuple *>(&id_data);
        if (!column_tuple || (column_tuple->tupleSize() != 2))
            return false;

        const auto * first = typeid_cast<const ColumnUInt64 *>(&column_tuple->getColumn(0));
        if (!first)
            return false;
        OneComponentID<UInt64> first_getter{first->getData().data()};

        const IColumn & second_column = column_tuple->getColumn(1);
        if (const auto * second_uint64 = typeid_cast<const ColumnUInt64 *>(&second_column))
        {
            func(TwoComponentID{first_getter, OneComponentID<UInt64>{second_uint64->getData().data()}});
            return true;
        }
        if (const auto * second_uint128 = typeid_cast<const ColumnUInt128 *>(&second_column))
        {
            func(TwoComponentID{first_getter, OneComponentID<UInt128>{second_uint128->getData().data()}});
            return true;
        }
        if (const auto * second_uuid = typeid_cast<const ColumnUUID *>(&second_column))
        {
            func(TwoComponentID{first_getter, OneComponentID<UUID>{second_uuid->getData().data()}});
            return true;
        }
        if (const auto * second_fixed_string = typeid_cast<const ColumnFixedString *>(&second_column))
        {
            if (second_fixed_string->getN() == sizeof(UInt128))
            {
                func(TwoComponentID{first_getter, FixedString16ID{second_fixed_string->getChars().data()}});
                return true;
            }
            return false;
        }

        /// A dictionary-encoded (LowCardinality) second component is read through its dictionary:
        /// the getter produces the same native representation as a plain column of the dictionary
        /// type, so these identifiers share their typed map with the plain layout (the store side
        /// receives materialized columns and picks that same map).
        if (const auto * second_low_cardinality = typeid_cast<const ColumnLowCardinality *>(&second_column))
        {
            if (second_low_cardinality->nestedIsNullable())
                return false;

            const IColumn & dictionary = *second_low_cardinality->getDictionary().getNestedColumn();
            bool dispatched = false;
            dispatchLowCardinalityIndexes(second_low_cardinality->getIndexes(), [&](const auto * index_data)
            {
                if (const auto * dictionary_uint64 = typeid_cast<const ColumnUInt64 *>(&dictionary))
                {
                    func(TwoComponentID{first_getter, LowCardinalityID{index_data, OneComponentID<UInt64>{dictionary_uint64->getData().data()}}});
                    dispatched = true;
                }
                else if (const auto * dictionary_uint128 = typeid_cast<const ColumnUInt128 *>(&dictionary))
                {
                    func(TwoComponentID{first_getter, LowCardinalityID{index_data, OneComponentID<UInt128>{dictionary_uint128->getData().data()}}});
                    dispatched = true;
                }
                else if (const auto * dictionary_uuid = typeid_cast<const ColumnUUID *>(&dictionary))
                {
                    func(TwoComponentID{first_getter, LowCardinalityID{index_data, OneComponentID<UUID>{dictionary_uuid->getData().data()}}});
                    dispatched = true;
                }
                else if (const auto * dictionary_fixed_string = typeid_cast<const ColumnFixedString *>(&dictionary))
                {
                    if (dictionary_fixed_string->getN() == sizeof(UInt128))
                    {
                        func(TwoComponentID{first_getter, LowCardinalityID{index_data, FixedString16ID{dictionary_fixed_string->getChars().data()}}});
                        dispatched = true;
                    }
                }
            });
            return dispatched;
        }

        return false;
    }

    /// For an id layout the typed dispatch does not handle, returns the column with any remaining
    /// dictionary encoding materialized (e.g. a LowCardinality first tuple component), to retry
    /// the dispatch with: the choice between a typed map and the generic map must match the store
    /// side, which receives materialized columns. Returns null if there is nothing to materialize.
    ColumnPtr tryMaterializeUnhandledLowCardinalityID(const IColumn & id_data)
    {
        ColumnPtr materialized = recursiveRemoveLowCardinality(id_data.getPtr());
        if (materialized.get() == &id_data)
            return nullptr;
        return materialized;
    }

    /// Serializes identifiers from a column to be used as keys in the mapping.
    /// Identifiers which are NULLs are skipped (their keys are left empty).
    VectorWithMemoryTracking<std::string_view> serializeIDs(const IColumn & id_data, const UInt8 * null_map, Arena & arena)
    {
        size_t num_rows = id_data.size();
        VectorWithMemoryTracking<std::string_view> keys;
        keys.resize(num_rows);
        for (size_t i = 0; i != num_rows; ++i)
        {
            if (null_map && null_map[i])
                continue;
            const char * begin = nullptr;
            keys[i] = id_data.serializeValueIntoArena(i, arena, begin, /* settings = */ nullptr);
        }
        return keys;
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
        std::unordered_set<std::string_view> tags_to_remove;
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
        std::unordered_set<std::string_view> tags_to_keep;
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
        std::vector<std::pair<std::string_view, std::string_view>> tags_to_copy;
        std::unordered_map<std::string_view, size_t> positions_in_tags_to_copy;
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
        std::unordered_map<std::string_view, std::vector<size_t>> positions_in_src_tags;
        std::vector<std::string_view> src_values;
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

                /// `$` with no name, or `${...` without a closing `}` — treat the `$` as literal.
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

String ContextTimeSeriesTagsCollector::toString(const TagNamesAndValuesPtr & tags)
{
    return toString(*tags);
}


ContextTimeSeriesTagsCollector::TagsKey::TagsKey(TagNamesAndValuesPtr tags_)
    : tags(std::move(tags_))
{
    hash = 0;
    for (const auto & [tag_name, tag_value] : *tags)
    {
        hash = CityHash_v1_0_2::CityHash64WithSeed(tag_name.data(), tag_name.length(), hash);
        hash = CityHash_v1_0_2::CityHash64WithSeed(tag_value.data(), tag_value.length(), hash);
    }
}


bool ContextTimeSeriesTagsCollector::Equal::operator()(const TagsKey & left, const TagsKey & right) const
{
    return *left.tags == *right.tags;
}


ContextTimeSeriesTagsCollector::ContextTimeSeriesTagsCollector()
{
    /// Group #0 is reserved for an empty set of tags.
    auto no_tags = std::make_shared<TagNamesAndValues>();
    auto group = tryAddGroupUnlocked(no_tags);
    chassert(group == getGroupForNoTags());
}


ContextTimeSeriesTagsCollector::~ContextTimeSeriesTagsCollector() = default;


Group ContextTimeSeriesTagsCollector::getGroupForTags(const TagNamesAndValuesPtr & tags)
{
    TagsKey key{tags};
    {
        SharedLockGuard lock{mutex};
        auto it = groups_for_tags.find(key);
        if (it != groups_for_tags.end())
            return it->second;
    }

    {
        std::lock_guard lock{mutex};
        return tryAddGroupUnlocked(std::move(key));
    }
}


VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::getGroupForTags(const VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector)
{
    VectorWithMemoryTracking<Group> res;
    res.resize(tags_vector.size(), INVALID_GROUP);
    size_t num_found = 0;

    std::vector<TagsKey> keys;
    keys.reserve(tags_vector.size());
    for (const auto & tags : tags_vector)
        keys.emplace_back(tags);

    {
        SharedLockGuard lock{mutex};
        for (size_t i = 0; i != tags_vector.size(); ++i)
        {
            auto it = groups_for_tags.find(keys[i]);
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
            res[i] = tryAddGroupUnlocked(std::move(keys[i]));
            if (++num_found == tags_vector.size())
                break;
        }
    }

    return res;
}


Group ContextTimeSeriesTagsCollector::tryAddGroupUnlocked(TagsKey && key)
{
    UInt64 hash = key.hash;
    TagNamesAndValuesPtr tags = key.tags;
    auto [it, inserted] = groups_for_tags.try_emplace(std::move(key), groups.size());
    if (inserted)
    {
        groups.push_back(std::move(tags));
        sampling_keys.push_back(hash);
    }
    return it->second;
}


Group ContextTimeSeriesTagsCollector::tryAddGroupUnlocked(const TagNamesAndValuesPtr & tags)
{
    return tryAddGroupUnlocked(TagsKey{tags});
}


TagNamesAndValuesPtr ContextTimeSeriesTagsCollector::getTagsByGroup(Group group) const
{
    SharedLockGuard lock{mutex};
    if (group >= groups.size())
        throwGroupOutOfBound(group, groups.size());
    return groups[group];
}


VectorWithMemoryTracking<TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByGroup(const VectorWithMemoryTracking<Group> & groups_) const
{
    VectorWithMemoryTracking<TagNamesAndValuesPtr> res;
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


UInt64 ContextTimeSeriesTagsCollector::getSamplingKeyByGroup(Group group) const
{
    SharedLockGuard lock{mutex};
    if (group >= sampling_keys.size())
        throwGroupOutOfBound(group, sampling_keys.size());
    return sampling_keys[group];
}


VectorWithMemoryTracking<UInt64> ContextTimeSeriesTagsCollector::getSamplingKeyByGroup(const VectorWithMemoryTracking<Group> & groups_) const
{
    VectorWithMemoryTracking<UInt64> res;
    res.resize(groups_.size());
    SharedLockGuard lock{mutex};
    for (size_t i = 0; i != groups_.size(); ++i)
    {
        Group group = groups_[i];
        if (group >= sampling_keys.size())
            throwGroupOutOfBound(group, sampling_keys.size());
        res[i] = sampling_keys[group];
    }
    return res;
}


String ContextTimeSeriesTagsCollector::extractTag(Group group, const String & tag_to_extract) const
{
    SharedLockGuard lock{mutex};
    if (group >= groups.size())
        throwGroupOutOfBound(group, groups.size());
    const auto & tags = *groups[group];
    for (const auto & [tag_name, tag_value] : tags)
    {
        if (tag_name == tag_to_extract)
            return tag_value;
    }
    return {};
}

VectorWithMemoryTracking<String> ContextTimeSeriesTagsCollector::extractTag(const VectorWithMemoryTracking<Group> & groups_, const String & tag_to_extract) const
{
    VectorWithMemoryTracking<String> res;
    res.resize(groups_.size());
    SharedLockGuard lock{mutex};
    for (size_t i = 0; i != groups_.size(); ++i)
    {
        Group group = groups_[i];
        if (group >= groups.size())
            throwGroupOutOfBound(group, groups.size());
        const auto & tags = *groups[group];
        for (const auto & [tag_name, tag_value] : tags)
        {
            if (tag_name == tag_to_extract)
            {
                res[i] = tag_value;
                break;
            }
        }
    }
    return res;
}

void ContextTimeSeriesTagsCollector::extractTag(
    const VectorWithMemoryTracking<Group> & groups_,
    const String & tag_to_extract,
    ColumnString & out_column,
    PaddedPODArray<UInt8> & null_map) const
{
    out_column.reserve(groups_.size());
    null_map.resize(groups_.size());
    SharedLockGuard lock{mutex};
    for (size_t i = 0; i != groups_.size(); ++i)
    {
        Group group = groups_[i];
        if (group >= groups.size())
            throwGroupOutOfBound(group, groups.size());
        const auto & tags = *groups[group];
        bool found = false;
        for (const auto & [tag_name, tag_value] : tags)
        {
            if (tag_name == tag_to_extract)
            {
                out_column.insertData(tag_value.data(), tag_value.size());
                null_map[i] = tag_value.empty();
                found = true;
                break;
            }
        }
        if (!found)
        {
            out_column.insertDefault();
            null_map[i] = 1;
        }
    }
}


template <typename IDType>
ContextTimeSeriesTagsCollector::IDMap<IDType> & ContextTimeSeriesTagsCollector::getTypedIDMap()
{
    if constexpr (std::is_same_v<IDType, UInt64>)
        return id_map_uint64;
    else if constexpr (std::is_same_v<IDType, UInt128>)
        return id_map_uint128;
    else if constexpr (std::is_same_v<IDType, std::pair<UInt64, UInt64>>)
        return id_map_pair_uint64_uint64;
    else
    {
        static_assert(std::is_same_v<IDType, std::pair<UInt64, UInt128>>);
        return id_map_pair_uint64_uint128;
    }
}


template <typename IDType>
const ContextTimeSeriesTagsCollector::IDMap<IDType> & ContextTimeSeriesTagsCollector::getTypedIDMap() const
{
    if constexpr (std::is_same_v<IDType, UInt64>)
        return id_map_uint64;
    else if constexpr (std::is_same_v<IDType, UInt128>)
        return id_map_uint128;
    else if constexpr (std::is_same_v<IDType, std::pair<UInt64, UInt64>>)
        return id_map_pair_uint64_uint64;
    else
    {
        static_assert(std::is_same_v<IDType, std::pair<UInt64, UInt128>>);
        return id_map_pair_uint64_uint128;
    }
}


void ContextTimeSeriesTagsCollector::storeTags(const ColumnPtr & id_column, const VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector)
{
    auto unwrapped = unwrapIDColumn(id_column);
    const NullMap * null_map = unwrapped.null_map;
    size_t num_rows = unwrapped.data->size();
    chassert(num_rows == tags_vector.size());

    size_t num_rows_to_store = num_rows;
    if (null_map)
        num_rows_to_store -= countBytesInFilter(*null_map);
    if (!num_rows_to_store)
        return;

    const UInt8 * null_map_data = null_map ? null_map->data() : nullptr;

    bool dispatched = dispatchIDType(*unwrapped.data, [&](const auto & id_getter)
    {
        storeTagsTyped(id_getter, *unwrapped.data, null_map_data, num_rows_to_store, tags_vector);
    });
    if (dispatched)
        return;

    if (ColumnPtr materialized = tryMaterializeUnhandledLowCardinalityID(*unwrapped.data))
    {
        dispatched = dispatchIDType(*materialized, [&](const auto & id_getter)
        {
            storeTagsTyped(id_getter, *materialized, null_map_data, num_rows_to_store, tags_vector);
        });
        if (!dispatched)
            storeTagsGeneric(*materialized, null_map_data, num_rows_to_store, tags_vector);
        return;
    }

    storeTagsGeneric(*unwrapped.data, null_map_data, num_rows_to_store, tags_vector);
}


template <typename IDGetter>
void ContextTimeSeriesTagsCollector::storeTagsTyped(
    const IDGetter & id_getter,
    const IColumn & id_data,
    const UInt8 * null_map,
    size_t num_rows_to_store,
    const VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector)
{
    using IDType = typename IDGetter::IDType;

    size_t num_rows = tags_vector.size();

    VectorWithMemoryTracking<Group> found_groups;
    found_groups.resize(num_rows, INVALID_GROUP);
    size_t num_found_groups = 0;

    /// Look up the ids which are already stored, under a shared lock.
    {
        SharedLockGuard lock{mutex};
        /// The cast selects the const overload of getTypedIDMap, which requires only a shared lock.
        const auto & id_map = const_cast<const ContextTimeSeriesTagsCollector *>(this)->getTypedIDMap<IDType>().map;

        for (size_t i = 0; i != num_rows; ++i)
        {
            if (null_map && null_map[i])
                continue;

            if (const auto * it = id_map.find(id_getter.get(i)))
            {
                Group existing_group = it->getMapped();
                if (*tags_vector[i] != *groups.at(existing_group))
                    throwIDWasAddedWithOtherTags(id_data, i, tags_vector[i], groups.at(existing_group));
                found_groups[i] = existing_group;
                ++num_found_groups;
            }
        }
    }

    if (num_found_groups == num_rows_to_store)
        return;

    {
        std::lock_guard lock{mutex};
        auto & id_map = getTypedIDMap<IDType>().map;

        for (size_t i = 0; i != num_rows; ++i)
        {
            if (null_map && null_map[i])
                continue;

            if (found_groups[i] != INVALID_GROUP)
                continue;

            Group group = tryAddGroupUnlocked(tags_vector[i]);

            typename HashMap<IDType, Group, IDMapHash>::LookupResult it = nullptr;
            bool inserted = false;
            id_map.emplace(id_getter.get(i), it, inserted);

            if (inserted)
                it->getMapped() = group;
            else if (it->getMapped() != group)
                throwIDWasAddedWithOtherTags(id_data, i, tags_vector[i], groups.at(it->getMapped()));
        }
    }
}


void ContextTimeSeriesTagsCollector::storeTagsGeneric(
    const IColumn & id_data,
    const UInt8 * null_map,
    size_t num_rows_to_store,
    const VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector)
{
    size_t num_rows = tags_vector.size();

    Arena temp_arena;
    auto ids = serializeIDs(id_data, null_map, temp_arena);

    VectorWithMemoryTracking<Group> found_groups;
    found_groups.resize(num_rows, INVALID_GROUP);
    size_t num_found_groups = 0;

    /// Look up the ids which are already stored, under a shared lock.
    {
        SharedLockGuard lock{mutex};

        for (size_t i = 0; i != num_rows; ++i)
        {
            const auto & id = ids[i];
            if (!id.empty())
            {
                if (const auto * it = generic_id_map.map.find(id))
                {
                    Group existing_group = it->getMapped();
                    if (*tags_vector[i] != *groups.at(existing_group))
                        throwIDWasAddedWithOtherTags(id_data, i, tags_vector[i], groups.at(existing_group));
                    found_groups[i] = existing_group;
                    ++num_found_groups;
                }
            }
        }
    }

    if (num_found_groups == num_rows_to_store)
        return;

    {
        std::lock_guard lock{mutex};

        for (size_t i = 0; i != num_rows; ++i)
        {
            const auto id = ids[i];
            if (id.empty())
                continue;

            if (found_groups[i] != INVALID_GROUP)
                continue;

            Group group = tryAddGroupUnlocked(tags_vector[i]);

            GenericIDMap::Map::LookupResult it = nullptr;
            bool inserted = false;
            generic_id_map.map.emplace(ArenaKeyHolder{id, generic_id_map.arena}, it, inserted);

            if (inserted)
                it->getMapped() = group;
            else if (it->getMapped() != group)
                throwIDWasAddedWithOtherTags(id_data, i, tags_vector[i], groups.at(it->getMapped()));
        }
    }
}


void ContextTimeSeriesTagsCollector::getGroupByID(const ColumnPtr & id_column, PaddedPODArray<Group> & res) const
{
    auto unwrapped = unwrapIDColumn(id_column);
    if (unwrapped.null_map)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected Nullable column {} of identifiers", id_column->getName());
    size_t num_rows = unwrapped.data->size();

    bool dispatched = dispatchIDType(*unwrapped.data, [&](const auto & id_getter)
    {
        getGroupByIDTyped(id_getter, *unwrapped.data, num_rows, res);
    });
    if (dispatched)
        return;

    if (ColumnPtr materialized = tryMaterializeUnhandledLowCardinalityID(*unwrapped.data))
    {
        dispatched = dispatchIDType(*materialized, [&](const auto & id_getter)
        {
            getGroupByIDTyped(id_getter, *materialized, num_rows, res);
        });
        if (!dispatched)
            getGroupByIDGeneric(*materialized, num_rows, res);
        return;
    }

    getGroupByIDGeneric(*unwrapped.data, num_rows, res);
}


template <typename IDGetter>
void ContextTimeSeriesTagsCollector::getGroupByIDTyped(
    const IDGetter & id_getter, const IColumn & id_data, size_t num_rows, PaddedPODArray<Group> & res) const
{
    using IDType = typename IDGetter::IDType;

    res.resize(num_rows);
    Group * __restrict out = res.data();

    SharedLockGuard lock{mutex};
    const auto & id_map = getTypedIDMap<IDType>().map;

    /// Id columns arrive in long runs of equal values (samples are sorted by id), so reuse the previous row's group.
    IDType prev_id{};
    Group prev_group = INVALID_GROUP;
    for (size_t i = 0; i != num_rows; ++i)
    {
        IDType id = id_getter.get(i);
        if ((id == prev_id) && (prev_group != INVALID_GROUP))
        {
            out[i] = prev_group;
            continue;
        }
        const auto * it = id_map.find(id);
        if (!it)
            throwUnknownID(id_data, i);
        prev_id = id;
        prev_group = it->getMapped();
        out[i] = prev_group;
    }
}


void ContextTimeSeriesTagsCollector::getGroupByIDGeneric(const IColumn & id_data, size_t num_rows, PaddedPODArray<Group> & res) const
{
    Arena temp_arena;

    res.resize(num_rows);
    Group * __restrict out = res.data();

    SharedLockGuard lock{mutex};

    /// Id columns arrive in long runs of equal values (samples are sorted by id), so reuse the previous row's group.
    Group prev_group = INVALID_GROUP;
    for (size_t i = 0; i != num_rows; ++i)
    {
        if ((i > 0) && id_data.compareAt(i, i - 1, id_data, /* nan_direction_hint = */ 1) == 0)
        {
            chassert(prev_group != INVALID_GROUP);
            out[i] = prev_group;
            continue;
        }
        const char * begin = nullptr;
        auto id = id_data.serializeValueIntoArena(i, temp_arena, begin, /* settings = */ nullptr);
        const auto * it = generic_id_map.map.find(id);
        if (!it)
            throwUnknownID(id_data, i);
        prev_group = it->getMapped();
        out[i] = prev_group;
    }
}


VectorWithMemoryTracking<TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByID(const ColumnPtr & id_column) const
{
    auto unwrapped = unwrapIDColumn(id_column);
    if (unwrapped.null_map)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected Nullable column {} of identifiers", id_column->getName());
    size_t num_rows = unwrapped.data->size();

    VectorWithMemoryTracking<TagNamesAndValuesPtr> res;
    bool dispatched = dispatchIDType(*unwrapped.data, [&](const auto & id_getter)
    {
        res = getTagsByIDTyped(id_getter, *unwrapped.data, num_rows);
    });
    if (dispatched)
        return res;

    if (ColumnPtr materialized = tryMaterializeUnhandledLowCardinalityID(*unwrapped.data))
    {
        dispatched = dispatchIDType(*materialized, [&](const auto & id_getter)
        {
            res = getTagsByIDTyped(id_getter, *materialized, num_rows);
        });
        if (dispatched)
            return res;

        return getTagsByIDGeneric(*materialized, num_rows);
    }

    return getTagsByIDGeneric(*unwrapped.data, num_rows);
}


template <typename IDGetter>
VectorWithMemoryTracking<TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByIDTyped(
    const IDGetter & id_getter, const IColumn & id_data, size_t num_rows) const
{
    using IDType = typename IDGetter::IDType;

    VectorWithMemoryTracking<TagNamesAndValuesPtr> res;
    res.reserve(num_rows);

    SharedLockGuard lock{mutex};
    const auto & id_map = getTypedIDMap<IDType>().map;

    /// Id columns arrive in long runs of equal values (samples are sorted by id), so reuse the previous row's tags.
    IDType prev_id{};
    TagNamesAndValuesPtr prev_tags;
    for (size_t i = 0; i != num_rows; ++i)
    {
        IDType id = id_getter.get(i);
        if ((id == prev_id) && prev_tags)
        {
            res.push_back(prev_tags);
            continue;
        }
        const auto * it = id_map.find(id);
        if (!it)
            throwUnknownID(id_data, i);
        prev_id = id;
        prev_tags = groups[it->getMapped()];
        res.push_back(prev_tags);
    }

    return res;
}


VectorWithMemoryTracking<TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByIDGeneric(
    const IColumn & id_data, size_t num_rows) const
{
    Arena temp_arena;
    auto keys = serializeIDs(id_data, nullptr, temp_arena);

    VectorWithMemoryTracking<TagNamesAndValuesPtr> res;
    res.reserve(num_rows);

    SharedLockGuard lock{mutex};

    for (size_t i = 0; i != num_rows; ++i)
    {
        const auto * it = generic_id_map.map.find(keys[i]);
        if (!it)
            throwUnknownID(id_data, i);
        res.push_back(groups[it->getMapped()]);
    }

    return res;
}


template <typename TransformFunc>
Group ContextTimeSeriesTagsCollector::transformTags(Group group, TransformFunc && transform_func)
{
    auto old_tags = getTagsByGroup(group);
    auto new_tags = transform_func(old_tags);
    if (*new_tags == *old_tags)
        return group;
    return getGroupForTags(new_tags);
}


template <typename TransformFunc>
VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::transformTags(const VectorWithMemoryTracking<Group> & groups_, TransformFunc && transform_func)
{
    if (groups_.empty())
        return {};

    auto tags_vector = getTagsByGroup(groups_);
    chassert(tags_vector.size() == groups_.size());

    VectorWithMemoryTracking<Group> res;
    res.resize(groups_.size());

    size_t num_new_tags = 0;

    auto [min_group_it, max_group_it] = std::minmax_element(groups_.begin(), groups_.end());
    Group min_group = *min_group_it;
    Group group_range = *max_group_it - min_group;

    /// Groups are dense integer indices, and a block usually contains a compact range of them.
    /// Avoid a large lookup array when a block contains only a sparse subset of all known groups.
    constexpr size_t max_dense_range_to_input_size_ratio = 4;
    bool use_dense_mapping = group_range / groups_.size() < max_dense_range_to_input_size_ratio;

    if (use_dense_mapping)
    {
        const size_t not_found = groups_.size();
        VectorWithMemoryTracking<size_t> indices_by_group;
        indices_by_group.resize(static_cast<size_t>(group_range) + 1, not_found);

        for (size_t i = 0; i != groups_.size(); ++i)
        {
            size_t & index = indices_by_group[groups_[i] - min_group];
            if (index == not_found)
            {
                index = num_new_tags;
                tags_vector[num_new_tags++] = transform_func(tags_vector[i]);
            }
            res[i] = index;
        }
    }
    else
    {
        std::unordered_map<Group, size_t> indices_by_group;

        for (size_t i = 0; i != groups_.size(); ++i)
        {
            Group group = groups_[i];
            auto [it, inserted] = indices_by_group.try_emplace(group, num_new_tags);
            if (inserted)
                tags_vector[num_new_tags++] = transform_func(tags_vector[i]);
            res[i] = it->second;
        }
    }

    tags_vector.resize(num_new_tags);

    auto new_groups = getGroupForTags(tags_vector);

    for (auto & index : res)
        index = new_groups.at(index);

    return res;
}


Group ContextTimeSeriesTagsCollector::removeTag(Group group, const String & tag_to_remove)
{
    return transformTags(group, RemoveTagTransformFunc{tag_to_remove});
}


VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::removeTag(const VectorWithMemoryTracking<Group> & groups_, const String & tag_to_remove)
{
    return transformTags(groups_, RemoveTagTransformFunc{tag_to_remove});
}


Group ContextTimeSeriesTagsCollector::removeTags(Group group, const Strings & tags_to_remove)
{
    return transformTags(group, RemoveTagsTransformFunc{tags_to_remove});
}


VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::removeTags(const VectorWithMemoryTracking<Group> & groups_, const Strings & tags_to_remove)
{
    return transformTags(groups_, RemoveTagsTransformFunc{tags_to_remove});
}


Group ContextTimeSeriesTagsCollector::removeAllTagsExcept(Group group, const Strings & tags_to_keep)
{
    return transformTags(group, RemoveAllTagsExceptTransformFunc{tags_to_keep});
}


VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::removeAllTagsExcept(const VectorWithMemoryTracking<Group> & groups_, const Strings & tags_to_keep)
{
    return transformTags(groups_, RemoveAllTagsExceptTransformFunc{tags_to_keep});
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
VectorWithMemoryTracking<Group>
ContextTimeSeriesTagsCollector::transformTags2(const VectorWithMemoryTracking<Group> & groups1, Group group2, TransformFunc2 && transform_func)
{
    return transformTags(
        groups1,
        TransformFunc2To1Adapter<TransformFunc2>
        {
            std::forward<TransformFunc2>(transform_func),
            /* other_argument = */ getTagsByGroup(group2),
            /* is_other_argument_second = */ true
        });
}


template <typename TransformFunc2>
VectorWithMemoryTracking<Group>
ContextTimeSeriesTagsCollector::transformTags2(Group group1, const VectorWithMemoryTracking<Group> & groups2, TransformFunc2 && transform_func)
{
    return transformTags(
        groups2,
        TransformFunc2To1Adapter<TransformFunc2>
        {
            std::forward<TransformFunc2>(transform_func),
            /* other_argument = */ getTagsByGroup(group1),
            /* is_other_argument_second = */ false
        });
}


template <typename TransformFunc2>
VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::transformTags2(const VectorWithMemoryTracking<Group> & groups1, const VectorWithMemoryTracking<Group> & groups2, TransformFunc2 && transform_func)
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

    VectorWithMemoryTracking<Group> res;
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


Group ContextTimeSeriesTagsCollector::copyTag(Group dest_group, Group src_group, const String & tag_to_copy)
{
    return transformTags2(dest_group, src_group, CopyTagTransformFunc2{tag_to_copy});
}


VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::copyTag(Group dest_group, const VectorWithMemoryTracking<Group> & src_groups, const String & tag_to_copy)
{
    return transformTags2(dest_group, src_groups, CopyTagTransformFunc2{tag_to_copy});
}


VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::copyTag(const VectorWithMemoryTracking<Group> & dest_groups, Group src_group, const String & tag_to_copy)
{
    return transformTags2(dest_groups, src_group, CopyTagTransformFunc2{tag_to_copy});
}


VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::copyTag(const VectorWithMemoryTracking<Group> & dest_groups, const VectorWithMemoryTracking<Group> & src_groups, const String & tag_to_copy)
{
    return transformTags2(dest_groups, src_groups, CopyTagTransformFunc2{tag_to_copy});
}


Group ContextTimeSeriesTagsCollector::copyTags(Group dest_group, Group src_group, const Strings & tags_to_copy)
{
    return transformTags2(dest_group, src_group, CopyTagsTransformFunc2{tags_to_copy});
}


VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::copyTags(Group dest_group, const VectorWithMemoryTracking<Group> & src_groups, const Strings & tags_to_copy)
{
    return transformTags2(dest_group, src_groups, CopyTagsTransformFunc2{tags_to_copy});
}


VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::copyTags(const VectorWithMemoryTracking<Group> & dest_groups, Group src_group, const Strings & tags_to_copy)
{
    return transformTags2(dest_groups, src_group, CopyTagsTransformFunc2{tags_to_copy});
}


VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::copyTags(const VectorWithMemoryTracking<Group> & dest_groups, const VectorWithMemoryTracking<Group> & src_groups, const Strings & tags_to_copy)
{
    return transformTags2(dest_groups, src_groups, CopyTagsTransformFunc2{tags_to_copy});
}


Group ContextTimeSeriesTagsCollector::joinTags(Group group, const String & dest_tag, const String & separator, const Strings & src_tags)
{
    return transformTags(group, JoinTagsTransformFunc{dest_tag, separator, src_tags});
}


VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::joinTags(const VectorWithMemoryTracking<Group> & groups_, const String & dest_tag, const String & separator, const Strings & src_tags)
{
    return transformTags(groups_, JoinTagsTransformFunc{dest_tag, separator, src_tags});
}


Group ContextTimeSeriesTagsCollector::replaceTag(Group group, const String & dest_tag, const String & replacement, const String & src_tag, const String & regex)
{
    return transformTags(group, ReplaceTagTransformFunc{dest_tag, replacement, src_tag, regex});
}


VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::replaceTag(const VectorWithMemoryTracking<Group> & groups_, const String & dest_tag, const String & replacement, const String & src_tag, const String & regex)
{
    return transformTags(groups_, ReplaceTagTransformFunc{dest_tag, replacement, src_tag, regex});
}


}
