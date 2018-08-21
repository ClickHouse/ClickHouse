#include <Common/Exception.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <Storages/System/StorageSystemNumbers.h>

namespace DB
{

class NumbersBlockInputStream : public IProfilingBlockInputStream
{
public:
    NumbersBlockInputStream(size_t block_size_, size_t offset_, size_t step_)
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
        UInt64 * pos = &vec[0]; /// This also accelerates the code.
        UInt64 * end = &vec[block_size];
        while (pos < end)
            *pos++ = curr++;

        next += step;
        return { ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeUInt64>(), "number") };
    }
private:
    size_t block_size;
    UInt64 next;
    UInt64 step;
};


StorageSystemNumbers::StorageSystemNumbers(const std::string & name_, bool multithreaded_, size_t limit_, size_t offset_)
    : name(name_), multithreaded(multithreaded_), limit(limit_), offset(offset_)
{
    setColumns(ColumnsDescription({{"number", std::make_shared<DataTypeUInt64>()}}));
}


BlockInputStreams StorageSystemNumbers::read(
    const Names & column_names,
    const SelectQueryInfo &,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    check(column_names);
    checkQueryProcessingStage(processed_stage, context);

    if (limit && limit < max_block_size)
    {
        max_block_size = std::min(max_block_size, limit);
        multithreaded = false;
    }

    if (!multithreaded)
        num_streams = 1;

    BlockInputStreams res(num_streams);
    for (size_t i = 0; i < num_streams; ++i)
    {
        res[i] = std::make_shared<NumbersBlockInputStream>(max_block_size, offset + i * max_block_size, num_streams * max_block_size);

        if (limit)  /// This formula is how to split 'limit' elements to 'num_streams' chunks almost uniformly.
            res[i] = std::make_shared<LimitBlockInputStream>(res[i], limit * (i + 1) / num_streams - limit * i / num_streams, 0);
    }

    return res;
}

}
