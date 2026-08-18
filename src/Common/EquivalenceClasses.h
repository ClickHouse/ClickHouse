#pragma once

#include <base/types.h>
#include <list>
#include <memory>
#include <unordered_map>

namespace DB
{

/// Tracks equivalence classes of values with transitive merging.
/// When two values are added as equivalent, they are placed in the same class.
/// Merging is transitive: if A==B and B==C, then A, B, C are all in the same class.
///
/// Implementation uses shared pointers to lists: all members of the same class
/// point to the same list object, enabling O(1) class lookup and efficient merging.
///
/// T must be equality-comparable; Hash must be a hash function for T.
template <typename T, typename Hash = std::hash<T>>
struct EquivalenceClasses
{
    using Class = std::list<T>;
    using ClassPtr = std::shared_ptr<Class>;
    using ConstClassPtr = std::shared_ptr<const Class>;

    void add(const T & a, const T & b)
    {
        auto & class_a = member_to_class[a];
        auto & class_b = member_to_class[b];

        if (!class_a && class_b)
        {
            /// Add A to existing class B
            class_a = class_b;
            class_b->push_back(a);
        }
        else if (class_a && !class_b)
        {
            /// Add B to existing class A
            class_b = class_a;
            class_a->push_back(b);
        }
        else if (!class_a && !class_b)
        {
            /// Both A and B are new, create a class for them
            auto new_class = std::make_shared<Class>();
            new_class->push_back(a);
            class_a = new_class;
            if (a != b)
            {
                new_class->push_back(b);
                class_b = new_class;
            }
        }
        else
        {
            /// A and B already belong to the same class?
            if (class_a == class_b)
                return;

            /// Merge class of smaller size into bigger one
            if (class_a->size() < class_b->size())
                mergeFromTo(class_a, class_b);
            else
                mergeFromTo(class_b, class_a);
        }
    }

    ConstClassPtr getClass(const T & name) const
    {
        auto it = member_to_class.find(name);
        if (it == member_to_class.end())
            return {};
        return it->second;
    }

    /// Merge all equivalence classes from another instance into this one.
    void merge(const EquivalenceClasses & other)
    {
        for (const auto & [member, class_ptr] : other.member_to_class)
        {
            if (class_ptr && class_ptr->size() >= 2)
            {
                auto first = class_ptr->front();
                for (auto it = std::next(class_ptr->begin()); it != class_ptr->end(); ++it)
                    add(first, *it);
            }
        }
    }

    const std::unordered_map<T, ClassPtr, Hash> & getMemberToClassMap() const
    {
        return member_to_class;
    }

private:
    void mergeFromTo(ClassPtr class_from, ClassPtr class_to)
    {
        /// For all existing members of class From set their class to To
        for (const auto & member_from : *class_from)
            member_to_class[member_from] = class_to;
        /// Add all elements from class From to class To
        class_to->splice(class_to->end(), *class_from);
    }

    /// Elements that belong to the same class will point to the same list of all elements of this class
    std::unordered_map<T, ClassPtr, Hash> member_to_class;
};

}
