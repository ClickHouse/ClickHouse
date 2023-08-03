#pragma once
#include <Core/Types.h>

namespace DB
{

template <typename T>
struct Vector2D : private std::vector<T>
{
    typedef std::vector<T> base_type;

    Vector2D(size_t h, size_t w) : base_type(h*w), height(h), width(w) {}

    T operator()(size_t i, size_t j) const
    {
        return base_type::operator[](i+j*height);
    }

    T& operator()(size_t i, size_t j)
    {
        return base_type::operator[](i+j*height);
    }
    void fill(T val)
    {
        for (size_t i = 0; i < height*width; i++)
            base_type::operator[](i) = val;
    }

private:
    size_t height, width;
};

std::vector<size_t> findGoodReordering(const std::vector<Float64> sizes,
                                              const std::vector<Float64> col_counts,
                                              const Vector2D<Float64> & selectivities,
                                              const std::vector<Float64> & self_selectivities);

}
