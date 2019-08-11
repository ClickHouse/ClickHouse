#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitors.h>
#include <Common/HashTable/Hash.h>
#include <Core/Defines.h>


using namespace DB;


void print(const ColumnPtr & col)
{
    size_t size = col->size();
    for (size_t i = 0; i < size; ++i)
        std::cerr << applyVisitor(FieldVisitorToString(), (*col)[i]) << ", ";
    std::cerr << '\n';
}

void print(const IColumn::BadHashes & hashes)
{
    size_t size = hashes.size();
    for (size_t i = 0; i < size; ++i)
        std::cerr << '(' << hashes[i].first << ", " << hashes[i].second << "), ";
    std::cerr << '\n';
}


int main(int, char **)
{
/*    for (size_t i = 0; i < 0x10000000000ULL; ++i)
        if (intHashCRC32(i) == 123)
            std::cerr << i << '\n';*/

    auto mut_col = ColumnUInt64::create();
    //for (auto v : std::initializer_list<UInt64>{10480126355, 1, 2, 3, 1, 2071127153, 2, 2, 1, 1, 6419288704})
    //    mut_col->insert(v);

    pcg64 rng;

    constexpr size_t size = 0x10000;
    auto & data = mut_col->getData();
    data.resize(size);
    for (size_t i = 0; i < size; ++i)
        data[i] = std::uniform_int_distribution<UInt64>(0, 1000)(rng);

    ColumnPtr col = std::move(mut_col);
//    print(col);

    IColumn::BadHashes hashes;
    col->calculateBadHashes(hashes);
//    print(hashes);

    IColumn::sortBadHashes(hashes);
//    print(hashes);

    col->disambiguateBadHashes(hashes);
//    print(hashes);

    return 0;
}
