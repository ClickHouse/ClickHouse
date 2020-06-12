#include <AggregateFunctions/QuantileTDigest.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

int main(int, char **)
{
    using namespace DB;

    QuantileTDigest<float> tdigest;
    tdigest.add(1);
    tdigest.add(2);
    tdigest.add(3);
    std::cout << tdigest.get(0.5) << "\n";
    WriteBufferFromOwnString wb;
    tdigest.serialize(wb);
    QuantileTDigest<float> other;
    ReadBufferFromString rb{wb.str()};
    other.deserialize(rb);
    std::cout << other.get(0.5) << "\n";

    return 0;
}
