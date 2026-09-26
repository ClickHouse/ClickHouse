#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

namespace DB
{

struct FsNode;

/// Persistent AVL tree: snapshots share unchanged entries. Updating one child copies
/// O(log n) entries instead of all n children of a directory containing many parts.
class FsDirectoryMap
{
    struct Entry;
    using EntryPtr = std::shared_ptr<const Entry>;
    using Value = std::shared_ptr<FsNode>;

    struct Entry
    {
        std::string name;
        Value value;
        EntryPtr left;
        EntryPtr right;
        int height;
    };

    EntryPtr root;

    static int height(const EntryPtr & entry)
    {
        return entry ? entry->height : 0;
    }

    static EntryPtr make(const std::string & name, const Value & value, EntryPtr left, EntryPtr right)
    {
        const int new_height = 1 + std::max(height(left), height(right));
        return std::make_shared<Entry>(name, value, std::move(left), std::move(right), new_height);
    }

    static EntryPtr balance(const std::string & name, const Value & value, EntryPtr left, EntryPtr right)
    {
        if (height(left) > height(right) + 1)
        {
            if (height(left->left) < height(left->right))
            {
                const auto & middle = left->right;
                return make(middle->name, middle->value,
                    make(left->name, left->value, left->left, middle->left),
                    make(name, value, middle->right, std::move(right)));
            }
            return make(left->name, left->value, left->left, make(name, value, left->right, std::move(right)));
        }
        if (height(right) > height(left) + 1)
        {
            if (height(right->right) < height(right->left))
            {
                const auto & middle = right->left;
                return make(middle->name, middle->value,
                    make(name, value, std::move(left), middle->left),
                    make(right->name, right->value, middle->right, right->right));
            }
            return make(right->name, right->value, make(name, value, std::move(left), right->left), right->right);
        }
        return make(name, value, std::move(left), std::move(right));
    }

    /// A null value removes an entry. Build the new root before publishing it so
    /// allocation failures leave this map and all its snapshots unchanged.
    static EntryPtr update(const EntryPtr & entry, const std::string & name, const Value & value)
    {
        if (!entry)
            return value ? make(name, value, nullptr, nullptr) : nullptr;

        if (name < entry->name)
            return balance(entry->name, entry->value, update(entry->left, name, value), entry->right);
        if (name > entry->name)
            return balance(entry->name, entry->value, entry->left, update(entry->right, name, value));
        if (value)
            return make(name, value, entry->left, entry->right);
        if (!entry->left)
            return entry->right;
        if (!entry->right)
            return entry->left;

        auto successor = entry->right;
        while (successor->left)
            successor = successor->left;
        return balance(successor->name, successor->value, entry->left, update(entry->right, successor->name, nullptr));
    }

    template <typename Callback>
    static void visit(const EntryPtr & entry, const Callback & callback)
    {
        if (!entry)
            return;
        visit(entry->left, callback);
        callback(entry->name, entry->value);
        visit(entry->right, callback);
    }

public:
    Value get(const std::string & name) const
    {
        const auto * entry = root.get();
        while (entry)
        {
            if (name == entry->name)
                return entry->value;
            entry = (name < entry->name ? entry->left : entry->right).get();
        }
        return nullptr;
    }

    void set(const std::string & name, const Value & value)
    {
        root = update(root, name, value);
    }
    void erase(const std::string & name)
    {
        root = update(root, name, nullptr);
    }
    bool empty() const
    {
        return !root;
    }

    template <typename Callback>
    void forEach(const Callback & callback) const
    {
        visit(root, callback);
    }
};

}
