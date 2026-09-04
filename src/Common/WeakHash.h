#pragma once
#include <Common/PODArray.h>

namespace DB
{

/// It's a class which represents the result of weak and fast hash function per row in column.
/// It's usually hardware accelerated CRC32-C.
/// Has function result may be combined to calculate hash for tuples.
///
/// The main purpose why this class needed is to support data initialization. Initially, every bit is 1.
class WeakHash32
{
public:
    static constexpr UInt32 kDefaultInitialValue = ~UInt32(0);

    using Container = PaddedPODArray<UInt32>;

    explicit WeakHash32(size_t size, UInt32 initial_value = kDefaultInitialValue) : data(size, initial_value) {}
    WeakHash32(const WeakHash32 & other) { data.assign(other.data); }

    void reset(size_t size, UInt32 initial_value = kDefaultInitialValue) { data.assign(size, initial_value); }

    void update(const WeakHash32 & other);

    const Container & getData() const { return data; }
    Container & getData() { return data; }

private:
    PaddedPODArray<UInt32> data;
};

}
