
#include <array>
#include <atomic>
#include <mutex>

namespace lfs
{

template <typename T>
class FixedSizeArray
{
public:
    FixedSizeArray() = default;
    explicit FixedSizeArray(size_t size_) : data_(new T[size_]) {}
    ~FixedSizeArray() { delete [] data_; }

    FixedSizeArray(const FixedSizeArray & other) = delete;
    FixedSizeArray & operator=(const FixedSizeArray &) = delete;

    FixedSizeArray(FixedSizeArray && other) noexcept { std::swap(data_, other.data_); }
    FixedSizeArray & operator=(FixedSizeArray && other) noexcept { std::swap(data_, other.data_); return *this; }

    T & operator[](size_t ps) { return data_[ps]; }
    const T & operator[](size_t ps) const { return data_[ps]; }

    T * data() { return data_; }

private:
    T * data_ = nullptr;
};

template <typename T>
class StackAllocator
{
public:
    explicit StackAllocator(size_t init_size = 127) { init(init_size); }

    uint32_t alloc()
    {
        uint64_t ps = size.fetch_add(1);

        if (__glibc_unlikely(ps >= capacity))
            allocCell(getCell(ps + 1));

        return ps;
    }

    T & operator[](uint32_t ps)
    {
        ++ps;
        return cells[getCell(ps)][ps];
    }

private:
    std::array<FixedSizeArray<T>, 32> data;
    std::array<T *, 32> cells;
    std::atomic<uint64_t> size {0};
    std::atomic<uint64_t> capacity {0};
    std::mutex mutex;

    size_t getCell(uint64_t ps) { return 63 - __builtin_clzll(ps); }  /// ps starts from 1 here.
    uint64_t getCellSize(size_t cell) { return 1ull << cell; }

    void init(size_t init_size)
    {
        cells.fill(nullptr);

        if (init_size == 0)
            return;

        /// Get Cell for the last value.
        size_t num_cells = getCell(init_size);

        data[0] = FixedSizeArray<T>(getCellSize(num_cells));
        T * ptr = data[0].data();

        for (size_t cell = 0; cell < num_cells; ++cell)
        {
            auto cell_size = getCellSize(cell);
            cells[cell] = ptr - cell_size;
            ptr += cell_size;
            capacity += cell_size;
        }
    }

    void allocCell(size_t cell)
    {
        if (cell >= cells.size())
            throw std::runtime_error("Not enough memory in StackAllocator.");

        std::lock_guard<std::mutex> guard(mutex);

        if (cells[cell] == nullptr)
        {
            auto cell_size = getCellSize(cell);
            data[cell] = FixedSizeArray<T>(cell_size);
            cells[cell] = data[cell].data() - cell_size;
            capacity += cell_size;
        }
    }
};

}
