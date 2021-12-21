#include "robin_hood.h"

template <bool IsFlat, size_t MaxLoadFactor100, typename Key, typename T, typename Hash,
          typename KeyEqual>
class IncrementalRehashTable 
{
public:
    static constexpr bool is_flat = IsFlat;
    static constexpr bool is_map = !std::is_void<T>::value;
    static constexpr bool is_set = !is_map;

    using key_type = Key;
    using mapped_type = T;
    using value_type = typename std::conditional<
        is_set, Key,
        robin_hood::pair<typename std::conditional<is_flat, Key, Key const>::type, T>>::type;
    using size_type = size_t;
    using hasher = Hash;
    using key_equal = KeyEqual;
    using Self = IncrementalRehashTable<IsFlat, MaxLoadFactor100, Key, T, Hash, KeyEqual>;
    using US = robin_hood::detail::Table<IsFlat, MaxLoadFactor100, Key, T, Hash, KeyEqual>;
    using inner_iterator = typename US::iterator;
private:
    std::shared_ptr<US> store[2];
    bool rehashing{false};
    inner_iterator cur; // rehash iterator
    float max_load_factor = static_cast<float>(MaxLoadFactor100 - 10)/100.0;
    std::size_t count{0};
public:
    IncrementalRehashTable()
    {
        store[0] = std::make_shared<US>();
        store[0]->reserve(1000000); // for big container, cost is memory
        store[1] = std::make_shared<US>();
        store[1]->reserve(2000000);
    }
    IncrementalRehashTable(const Self & rhs) {
        store[0] = rhs.store[0];
        store[1] = rhs.store[1];
        rehashing = rhs.rehashing;
        cur = rhs.cur;
        max_load_factor = rhs.max_load_factor;
        count = rhs.count;
    }
    ~IncrementalRehashTable() = default;
    void reseve(size_t t)
    {
        store[0]->reserve(t);
        store[1]->reserve(t * 2);
    }
    template <bool IsConst>
    struct Iter
    {
    public:
        using UsPtr = typename std::conditional<IsConst, Self const*, Self*>::type;
        using difference_type = std::ptrdiff_t;
        using value_type = typename Self::value_type;
        using reference = typename std::conditional<IsConst, value_type const&, value_type&>::type;
        using pointer = typename std::conditional<IsConst, value_type const*, value_type*>::type;
        using iterator_category = std::forward_iterator_tag;
        using inner_iter = typename std::conditional<IsConst, typename US::const_iterator, typename US::iterator>::type;

        Iter() = default;
        Iter(UsPtr ptr, inner_iter iter, int idx) : ref(ptr), m_iter(iter), index(idx) {}

        reference operator*() const { return *m_iter; }
        pointer operator->() { return &*m_iter; }
        Iter& operator++() noexcept
        {
            ++m_iter;
            if (index == 0 && ref->rehashing && m_iter == ref->store[index]->end())
            {
                m_iter = ref->store[1]->begin();
            }
            return *this;
        }
        Iter operator++(int) noexcept { Iter tmp = *this; ++(*this); return tmp; }
        template <bool O>
        bool operator==(Iter<O> const& o) const noexcept
        {
            return m_iter == o.m_iter;
        }
        template <bool O>
        bool operator!=(Iter<O> const& o) const noexcept
        {
            return m_iter != o.m_iter;
        }
        UsPtr ref;
        inner_iter m_iter;
        int index{0};
    };
    using iterator = Iter<false>;
    using const_iterator = Iter<true>;

    void moveOneEntry()
    {
        store[1]->insert(*cur);
        if (++cur == store[0]->end())
        {
            rehashing = false;
            store[0] = store[1];
            store[1] = std::make_shared<US>();
            store[1]->reserve(store[0]->size() * 2);
            std::cout << "rehash end, size " << store[0]->size() << ", count " << count << std::endl;
        }
    }
    // only insert table 1 if rehashing
    std::pair<iterator,bool> insert(const value_type & t)
    {
        ++count;
        if (rehashing)
        {
            auto res = store[1]->insert(t);
            moveOneEntry();
            return std::make_pair(iterator(this, res.first, 1), res.second);
        }
        if (store[0]->load_factor() > max_load_factor)
        {
            std::cout << "rehash begin, size " << store[0]->size() << ", count " << count << std::endl;
            rehashing = true;
            cur = store[0]->begin();
            store[1]->reserve(store[0]->size()*2);
            moveOneEntry();
            auto res = store[1]->insert(t);
            return std::make_pair(iterator(this, res.first, 1), res.second);
        }
        auto res = store[0]->insert(t);
        return std::make_pair(iterator(this, res.first, 0), res.second);
    }
    void erase(const Key & t)
    {
        std::size_t del = 0;
        del = store[0]->erase(t);
        if (rehashing)
        {
            del += store[1]->erase(t);
            moveOneEntry();
        }
        if (del) --count;
    }
    std::size_t size() const
    {
        //std::cout << "hash 0 " << store[0]->size() << ", hash 1 " << store[1]->size() << std::endl;
        return count;
    }
    bool empty() const { return count == 0; }
    const_iterator find(const Key & key) const
    {
        auto res =  store[0]->find(key);
        if (res != store[0]->end())
            return const_iterator(this, res, 0);
        if (rehashing)
            return const_iterator(this, store[1]->find(key), 1);
        return const_iterator(this, store[0]->end(), 0);
    }
    bool contains(const Key & t) const
    {
        if (store[0]->contains(t))
            return true;
        if (rehashing && store[1]->contains(t)) {
            return true;
        }
        return false;
    }
    const_iterator begin() const
    {
        return const_iterator(this, store[0]->begin(), 0);
    }
    iterator begin()
    {
        return iterator(this, store[0]->begin(), 0);
    }
    const_iterator end() const
    {
        if (rehashing) {
            return const_iterator(this, store[1]->end(), 1); 
        }
        return const_iterator(this, store[0]->end(), 0); 
    }
    size_t htsize()
    { 
        return store[0]->size() + (rehashing ? store[1]->size() : 0);
    }
};

template <typename Key, typename Hash = robin_hood::hash<Key>, typename KeyEqual = std::equal_to<Key>,
          size_t MaxLoadFactor100 = 80>
using my_unordered_set = IncrementalRehashTable<sizeof(Key) <= sizeof(size_t) * 6 &&
                                        std::is_nothrow_move_constructible<Key>::value &&
                                        std::is_nothrow_move_assignable<Key>::value,
                                    MaxLoadFactor100, Key, void, Hash, KeyEqual>;

template <typename Key, typename T, typename Hash = robin_hood::hash<Key>,
          typename KeyEqual = std::equal_to<Key>, size_t MaxLoadFactor100 = 80>
using my_unordered_map = IncrementalRehashTable<sizeof(robin_hood::pair<Key, T>) <= sizeof(size_t) * 6 &&
                      std::is_nothrow_move_constructible<robin_hood::pair<Key, T>>::value &&
                      std::is_nothrow_move_assignable<robin_hood::pair<Key, T>>::value,
                  MaxLoadFactor100, Key, T, Hash, KeyEqual>;
