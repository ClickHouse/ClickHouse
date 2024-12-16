#pragma once
#include <Interpreters/HashJoin/HashJoin.h>
#include <Common/ColumnsHashingImpl.h>
#include <Interpreters/RowRefs.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
namespace DB
{

template <bool flag_per_row>
class KnownRowsHolder;

/// Keep already joined rows to prevent duplication if many disjuncts
///   if for a particular pair of rows condition looks like TRUE or TRUE or TRUE
///   we want to have it once in resultset
template<>
class KnownRowsHolder<true>
{
public:
    using Type = std::pair<const Block *, DB::RowRef::SizeT>;

private:
    static const size_t MAX_LINEAR = 16; // threshold to switch from Array to Set
    using ArrayHolder = std::array<Type, MAX_LINEAR>;
    using SetHolder = std::set<Type>;
    using SetHolderPtr = std::unique_ptr<SetHolder>;

    ArrayHolder array_holder;
    SetHolderPtr set_holder_ptr;

    size_t items;

public:
    KnownRowsHolder()
        : items(0)
    {
    }


    template<class InputIt>
    void add(InputIt from, InputIt to)
    {
        const size_t new_items = std::distance(from, to);
        if (items + new_items <= MAX_LINEAR)
        {
            std::copy(from, to, &array_holder[items]);
        }
        else
        {
            if (items <= MAX_LINEAR)
            {
                set_holder_ptr = std::make_unique<SetHolder>();
                set_holder_ptr->insert(std::cbegin(array_holder), std::cbegin(array_holder) + items);
            }
            set_holder_ptr->insert(from, to);
        }
        items += new_items;
    }

    template<class Needle>
    bool isKnown(const Needle & needle)
    {
        return items <= MAX_LINEAR
            ? std::find(std::cbegin(array_holder), std::cbegin(array_holder) + items, needle) != std::cbegin(array_holder) + items
            : set_holder_ptr->find(needle) != set_holder_ptr->end();
    }
};

template<>
class KnownRowsHolder<false>
{
public:
    template<class InputIt>
    void add(InputIt, InputIt)
    {
    }

    template<class Needle>
    static bool isKnown(const Needle &)
    {
        return false;
    }
};

template <typename Mapped, bool need_offset = false>
using FindResultImpl = ColumnsHashing::columns_hashing_impl::FindResultImpl<Mapped, true>;


template <typename Map, bool add_missing, bool flag_per_row, typename AddedColumns>
void addFoundRowAll(
    const typename Map::mapped_type & mapped,
    AddedColumns & added,
    IColumn::Offset & current_offset,
    KnownRowsHolder<flag_per_row> & known_rows [[maybe_unused]],
    JoinStuff::JoinUsedFlags * used_flags [[maybe_unused]])
{
    if constexpr (add_missing)
        added.applyLazyDefaults();

    if constexpr (flag_per_row)
    {
        std::unique_ptr<std::vector<KnownRowsHolder<true>::Type>> new_known_rows_ptr;

        for (auto it = mapped.begin(); it.ok(); ++it)
        {
            if (!known_rows.isKnown(std::make_pair(it->block, it->row_num)))
            {
                added.appendFromBlock(*it, false);
                ++current_offset;
                if (!new_known_rows_ptr)
                {
                    new_known_rows_ptr = std::make_unique<std::vector<KnownRowsHolder<true>::Type>>();
                }
                new_known_rows_ptr->push_back(std::make_pair(it->block, it->row_num));
                if (used_flags)
                {
                    used_flags->JoinStuff::JoinUsedFlags::setUsedOnce<true, flag_per_row>(
                        FindResultImpl<const RowRef, false>(*it, true, 0));
                }
            }
        }

        if (new_known_rows_ptr)
        {
            known_rows.add(std::cbegin(*new_known_rows_ptr), std::cend(*new_known_rows_ptr));
        }
    }
    else if constexpr (AddedColumns::isLazy())
    {
        added.appendFromBlock(&mapped, false);
        current_offset += mapped.rows;
    }
    else
    {
        for (auto it = mapped.begin(); it.ok(); ++it)
        {
            added.appendFromBlock(*it, false);
            ++current_offset;
        }
    }
}

template <bool add_missing, bool need_offset, typename AddedColumns>
void addNotFoundRow(AddedColumns & added [[maybe_unused]], IColumn::Offset & current_offset [[maybe_unused]])
{
    if constexpr (add_missing)
    {
        added.appendDefaultRow();
        if constexpr (need_offset)
            ++current_offset;
    }
}

}
