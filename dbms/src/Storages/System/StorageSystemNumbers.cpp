#include <Common/Exception.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <Storages/System/StorageSystemNumbers.h>

namespace DB
{

class NumbersBlockInputStream : public IBlockInputStream
{
public:
    NumbersBlockInputStream(UInt64 block_size_, UInt64 offset_, UInt64 step_)
        : block_size(block_size_), next(offset_), step(step_) {}

    String getName() const override { return "Numbers"; }

    Block getHeader() const override
    {
        return { ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number") };
    }

protected:
    Block readImpl() override
    {
        auto column = ColumnUInt64::create(block_size);
        ColumnUInt64::Container & vec = column->getData();

        size_t curr = next;     /// The local variable for some reason works faster (>20%) than member of class.
        UInt64 * pos = vec.data(); /// This also accelerates the code.
        UInt64 * end = &vec[block_size];
        while (pos < end)
            *pos++ = curr++;

        next += step;
        return { ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeUInt64>(), "number") };
    }
private:
    UInt64 block_size;
    UInt64 next;
    UInt64 step;
};


StorageSystemNumbers::StorageSystemNumbers(const std::string & name_, bool multithreaded_, std::optional<UInt64> limit_, UInt64 offset_)
    : name(name_), multithreaded(multithreaded_), limit(limit_), offset(offset_)
{
    setColumns(ColumnsDescription({{"number", std::make_shared<DataTypeUInt64>()}}));
}

BlockInputStreams StorageSystemNumbers::read(
    const Names & column_names,
    const SelectQueryInfo &,
    const Context & /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    check(column_names);

    if (limit && *limit < max_block_size)
    {
        max_block_size = static_cast<size_t>(*limit);
        multithreaded = false;
    }

    if (!multithreaded)
        num_streams = 1;

    BlockInputStreams res(num_streams);
    for (size_t i = 0; i < num_streams; ++i)
    {
        res[i] = std::make_shared<NumbersBlockInputStream>(max_block_size, offset + i * max_block_size, num_streams * max_block_size);

        if (limit)  /// This formula is how to split 'limit' elements to 'num_streams' chunks almost uniformly.
            res[i] = std::make_shared<LimitBlockInputStream>(res[i], *limit * (i + 1) / num_streams - *limit * i / num_streams, 0,  false, true);
    }

    return res;
}

}
