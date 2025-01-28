#pragma once

#include <type_traits>
#include <Common/FieldVisitors.h>
#include <Core/Field.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

bool isFixedSizeFieldType(Field::Types::Which type);

/// Returns the size in bytes of the field.
/// If @allocated is true then returns the number of allocated bytes.
/// The returned size is always not less than sizeof(Field)
template <bool allocated = false>
class FieldVisitorByteSize : public StaticVisitor<UInt64>
{
public:
    /// If T is trivially copyable, it doesn't allocate any external memory.
    /// However, the field size is not sizeof(T) because we always allocate the memory,
    /// which is enough for the largest possible Field types (see Field::storage).
    template <typename T> requires std::is_trivially_copyable_v<T>
    UInt64 operator() (const T &)
    {
        return sizeof(Field);
    }

    UInt64 operator() (const String & x) const
    {
        return sizeof(Field) + byteSizeOfString(x);
    }

    template <typename T> requires std::is_base_of_v<FieldVector, T>
    UInt64 operator() (const T & x) const
    {
        UInt64 res = sizeof(Field);
        size_t size = x.size();

        if constexpr (allocated)
            res += (x.capacity() - size) * sizeof(Field);

        if (x.empty())
            return res;

        if (isFixedSizeFieldType(x[0].getType()))
            return res + size * sizeof(Field);

        FieldVisitorByteSize<allocated> visitor;
        for (const auto & elem : x)
            res += applyVisitor(visitor, elem);

        return res;
    }

    UInt64 operator() (const AggregateFunctionStateData & x) const
    {
        return sizeof(Field) + byteSizeOfString(x.name) + byteSizeOfString(x.data);
    }

    UInt64 operator() (const Object & x) const
    {
        UInt64 res = sizeof(Field);

        if (x.empty())
            return res;

        if (isFixedSizeFieldType(x.begin()->second.getType()))
        {
            for (const auto & [key, _] : x)
                res += byteSizeOfString(key);

            return res + x.size() * sizeof(Object::value_type);
        }

        FieldVisitorByteSize<allocated> visitor;

        for (const auto & [key, value] : x)
        {
            res += byteSizeOfString(key);
            res += applyVisitor(visitor, value);
        }

        /// Sizes of element type are already counted by visitor.
        return res + x.size() * sizeof(Object::key_type);
    }

    UInt64 operator() (const CustomType &) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get size if bytes of CustomType");
    }

private:
    UInt64 byteSizeOfString(const String & str) const
    {
        /// If string uses small string optimization then no extra memory is allocated.
        /// Use the capacity of empty string as a max length for SSO (see the implementation of std::string::capacity).
        static constexpr size_t max_sso_size = std::string().capacity();

        if (str.capacity() <= max_sso_size)
            return 0;

        if constexpr (allocated)
            return str.capacity();
        else
            return str.size();
    }
};

using FieldVisitorAllocatedBytes = FieldVisitorByteSize<true>;

}
