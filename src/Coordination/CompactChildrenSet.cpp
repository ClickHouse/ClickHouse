#include <Coordination/CompactChildrenSet.h>

#include <Common/Exception.h>

#include <base/defines.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

// --- Iterator implementation ---

CompactChildrenSet::ConstIterator::ConstIterator(std::string_view sv_, bool done_)
    : set_mode(false), done(done_), sv(sv_)
{
}

CompactChildrenSet::ConstIterator::ConstIterator(ChildrenSet::const_iterator set_it_)
    : set_mode(true), done(false), set_it(set_it_)
{
}

CompactChildrenSet::ConstIterator::reference CompactChildrenSet::ConstIterator::operator*() const
{
    if (set_mode)
        return *set_it;
    return sv;
}

CompactChildrenSet::ConstIterator::pointer CompactChildrenSet::ConstIterator::operator->() const
{
    if (set_mode)
        return &*set_it;
    return &sv;
}

CompactChildrenSet::ConstIterator & CompactChildrenSet::ConstIterator::operator++()
{
    if (set_mode)
        ++set_it;
    else
        done = true;
    return *this;
}

CompactChildrenSet::ConstIterator CompactChildrenSet::ConstIterator::operator++(int)
{
    auto tmp = *this;
    ++(*this);
    return tmp;
}

bool CompactChildrenSet::ConstIterator::operator==(const ConstIterator & other) const
{
    if (set_mode != other.set_mode)
        return false;
    if (set_mode)
        return set_it == other.set_it;
    return done == other.done;
}

bool CompactChildrenSet::ConstIterator::operator!=(const ConstIterator & other) const
{
    return !(*this == other);
}

// --- CompactChildrenSet implementation ---

CompactChildrenSet::~CompactChildrenSet()
{
    if (isSet())
        delete asSet();
}

CompactChildrenSet::CompactChildrenSet(const CompactChildrenSet & other)
{
    if (other.isSet())
    {
        auto * new_set = new ChildrenSet(*other.asSet());
        ptr = reinterpret_cast<const char *>(new_set);
        name_size = 0;
    }
    else
    {
        ptr = other.ptr;
        name_size = other.name_size;
    }
}

CompactChildrenSet & CompactChildrenSet::operator=(const CompactChildrenSet & other)
{
    if (this == &other)
        return *this;

    /// Allocate-then-swap for exception safety (§4.2)
    if (other.isSet())
    {
        auto * new_set = new ChildrenSet(*other.asSet());
        if (isSet())
            delete asSet();
        ptr = reinterpret_cast<const char *>(new_set);
        name_size = 0;
    }
    else
    {
        if (isSet())
            delete asSet();
        ptr = other.ptr;
        name_size = other.name_size;
    }
    return *this;
}

CompactChildrenSet::CompactChildrenSet(CompactChildrenSet && other) noexcept
    : ptr(other.ptr), name_size(other.name_size)
{
    other.ptr = nullptr;
    other.name_size = 0;
}

CompactChildrenSet & CompactChildrenSet::operator=(CompactChildrenSet && other) noexcept
{
    if (this == &other)
        return *this;

    if (isSet())
        delete asSet();

    ptr = other.ptr;
    name_size = other.name_size;
    other.ptr = nullptr;
    other.name_size = 0;
    return *this;
}

void CompactChildrenSet::insert(std::string_view child)
{
    if (child.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty child names are not allowed in CompactChildrenSet");

    if (isEmpty())
    {
        ptr = child.data();
        name_size = child.size();
        return;
    }

    if (isSingle())
    {
        std::string_view existing = asSingle();
        if (existing == child)
            return;
        promoteToSet(existing, child);
        return;
    }

    /// Set mode
    asSet()->insert(child);
}

void CompactChildrenSet::erase(std::string_view child)
{
    if (isEmpty())
        return;

    if (isSingle())
    {
        if (asSingle() == child)
        {
            ptr = nullptr;
            name_size = 0;
        }
        return;
    }

    /// Set mode
    auto * set = asSet();
    set->erase(child);

    if (set->size() == 1)
    {
        /// Demote to single mode.
        /// The string_view points to arena-owned keys in SnapshotableHashTable
        /// that outlive this set, so it's safe to keep the pointer.
        std::string_view remaining = *set->begin();
        chassert(!remaining.empty());
        delete set;
        ptr = remaining.data();
        name_size = remaining.size();
    }
    else if (set->empty())
    {
        delete set;
        ptr = nullptr;
        name_size = 0;
    }
}

size_t CompactChildrenSet::size() const
{
    if (isEmpty())
        return 0;
    if (isSingle())
        return 1;
    return asSet()->size();
}

bool CompactChildrenSet::empty() const
{
    if (isSet())
        /// `reserve(n>=2)` on an empty container promotes to set mode with zero elements.
        /// This is the only way to reach a zero-element set.
        return asSet()->empty();
    return isEmpty();
}

bool CompactChildrenSet::contains(std::string_view child) const
{
    if (isEmpty())
        return false;
    if (isSingle())
        return asSingle() == child;
    return asSet()->contains(child);
}

void CompactChildrenSet::clear()
{
    if (isSet())
        delete asSet();
    ptr = nullptr;
    name_size = 0;
}

void CompactChildrenSet::reserve(size_t n)
{
    if (n <= 1)
        return;

    if (isSet())
    {
        asSet()->reserve(n);
        return;
    }

    /// Need to promote to set mode (§4.6)
    auto set = std::make_unique<ChildrenSet>();
    if (isSingle())
        set->insert(asSingle());
    set->reserve(n);
    ptr = reinterpret_cast<const char *>(set.release());
    name_size = 0;
}

CompactChildrenSet::ConstIterator CompactChildrenSet::begin() const
{
    if (isEmpty())
        return ConstIterator({}, true);

    if (isSingle())
        return ConstIterator(asSingle(), false);

    return ConstIterator(asSet()->begin());
}

CompactChildrenSet::ConstIterator CompactChildrenSet::end() const
{
    if (isSet())
        return ConstIterator(asSet()->end());

    return ConstIterator({}, true);
}

size_t CompactChildrenSet::heapSizeInBytes() const
{
    if (isSet())
        return sizeof(ChildrenSet) + asSet()->capacity() * sizeof(std::string_view);
    return 0;
}

void CompactChildrenSet::promoteToSet(std::string_view existing, std::string_view new_child)
{
    auto set = std::make_unique<ChildrenSet>();
    set->insert(existing);
    set->insert(new_child);
    ptr = reinterpret_cast<const char *>(set.release());
    name_size = 0;
}

}
