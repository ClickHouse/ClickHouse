#pragma once

#include <base/types.h>
#include <unordered_map>
#include <memory>
#include <list>

namespace DB
{

struct EquivalenceClasses
{
    void add(const String & a, const String & b)
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
            auto new_class = std::make_shared<std::list<String>>();
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

    std::shared_ptr<const std::list<String>> getClass(const String & name) const
    {
        auto it = member_to_class.find(name);
        if (it == member_to_class.end())
            return {};
        return it->second;
    }

    const std::unordered_map<String, std::shared_ptr<std::list<String>>> & getMembers() const
    {
        return member_to_class;
    }

private:
    void mergeFromTo(std::shared_ptr<std::list<String>> class_from, std::shared_ptr<std::list<String>> class_to)
    {
        /// For all existing members of class From set their class to To
        for (const auto & member_from : *class_from)
            member_to_class[member_from] = class_to;
        /// Add all elements from class From to class To
        class_to->splice(class_to->end(), *class_from);
    }

    /// Elements that belong to the same class will point to the same list of all elements of this class
    std::unordered_map<String, std::shared_ptr<std::list<String>>> member_to_class;
};

}
