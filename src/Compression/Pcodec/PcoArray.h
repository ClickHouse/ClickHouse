#pragma once

/** Container type for the codec's internal buffers.
  *
  * In the ClickHouse build this is a thin facade over `DB::PODArray`, so all codec allocations go
  * through ClickHouse's memory-tracked allocator (as required by the `src/Compression` style lint,
  * which forbids raw `std::vector`). The facade restores the `std::vector` semantics the codec
  * relies on that plain `PODArray` lacks: it is copyable, and `resize(n)` / `PcoArray(n)`
  * value-initialize (zero) new elements.
  *
  * The standalone cross-validation harnesses compile these headers without the ClickHouse runtime
  * (no allocator, no boost), so under `PCODEC_STANDALONE` the container falls back to `std::vector`.
  * Because the facade mirrors `std::vector` semantics exactly, the two builds behave identically,
  * which keeps the reference-`.pco` oracle a faithful check of the production code path.
  */

#ifdef PCODEC_STANDALONE

#include <vector>
namespace DB::Pcodec
{
template <typename T> using PcoArray = std::vector<T>; // STYLE_CHECK_ALLOW_STD_CONTAINERS
}

#else

#include <Common/PODArray.h>

#include <initializer_list>
#include <utility>

namespace DB::Pcodec
{

/// std::vector-compatible facade over PODArray (copyable; value-initializing resize/sized-ctor).
template <typename T>
class PcoArray : public DB::PODArray<T>
{
public:
    using Base = DB::PODArray<T>;

    PcoArray() = default;
    explicit PcoArray(size_t n) { this->resize_fill(n); }
    PcoArray(size_t n, const T & value) : Base(n, value) { }
    PcoArray(const T * from, const T * to) : Base(from, to) { }
    PcoArray(std::initializer_list<T> il) : Base(il) { }

    PcoArray(const PcoArray & other) { this->assign(other); }
    PcoArray(PcoArray && other) noexcept : Base(std::move(static_cast<Base &>(other))) { }

    PcoArray & operator=(const PcoArray & other)
    {
        if (this != &other)
            this->assign(other);
        return *this;
    }
    PcoArray & operator=(PcoArray && other) noexcept
    {
        Base::operator=(std::move(static_cast<Base &>(other)));
        return *this;
    }

    /// Value-initialize (zero) newly added elements, matching std::vector::resize.
    void resize(size_t n) { this->resize_fill(n); }
    void resize(size_t n, const T & value) { this->resize_fill(n, value); }
};

}

#endif
