#include <Interpreters/RowRefs.h>

#include <Common/ColumnsHashing.h>
#include <Core/Block.h>
#include <Columns/IColumn.h>

#include <optional>

namespace DB
{

void AsofRowRefs::Lookups::create(AsofRowRefs::Type which)
{
    switch (which)
    {
        case Type::EMPTY: break;
    #define M(NAME, TYPE) \
        case Type::NAME: NAME = std::make_unique<typename decltype(NAME)::element_type>(); break;
            APPLY_FOR_ASOF_JOIN_VARIANTS(M)
    #undef M
    }
}

template<typename T>
using AsofGetterType = ColumnsHashing::HashMethodOneNumber<T, T, T, false>;

void AsofRowRefs::insert(const IColumn * asof_column, const Block * block, size_t row_num, Arena & pool)
{
    switch (type)
    {
        case Type::EMPTY: break;
    #define M(NAME, TYPE) \
        case Type::NAME: {  \
            auto asof_getter = AsofGetterType<TYPE>(asof_column); \
            auto entry = Entry<TYPE>(asof_getter.getKey(row_num, pool), RowRef(block, row_num));  \
            lookups.NAME->insert(entry); \
            break;    \
        }
            APPLY_FOR_ASOF_JOIN_VARIANTS(M)
    #undef M
    }
}

const RowRef * AsofRowRefs::findAsof(const IColumn * asof_column, size_t row_num, Arena & pool) const
{
    switch (type)
    {
        case Type::EMPTY: return nullptr;
    #define M(NAME, TYPE) \
        case Type::NAME: {  \
            auto asof_getter = AsofGetterType<TYPE>(asof_column); \
            TYPE key = asof_getter.getKey(row_num, pool);   \
            auto it = lookups.NAME->upper_bound(Entry<TYPE>(key));   \
            if (it == lookups.NAME->cbegin())  \
                return nullptr;  \
            return &((--it)->row_ref); \
        }
            APPLY_FOR_ASOF_JOIN_VARIANTS(M)
    #undef M
    }

    __builtin_unreachable();
}

std::optional<std::pair<AsofRowRefs::Type, size_t>> AsofRowRefs::getTypeSize(const IColumn * asof_column)
{
    #define M(NAME, TYPE) \
    if (strcmp(#TYPE, asof_column->getFamilyName()) == 0) \
        return std::make_pair(Type::NAME,sizeof(TYPE));
    APPLY_FOR_ASOF_JOIN_VARIANTS(M)
    #undef M
    return {};
}

}
