#include <Columns/ColumnQBit.h>
#include <Columns/ColumnsView.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <gtest/gtest.h>

using namespace DB;

TEST(ColumnQBit, PrepareForSquashingDirectSingleSource)
{
    auto type = DataTypeFactory::instance().get("QBit(Float32, 8)");
    auto mutable_source = type->createColumn();
    mutable_source->insert(type->getDefault());
    mutable_source->insert(type->getDefault());
    ColumnPtr source = std::move(mutable_source);

    auto target = type->createColumn();
    static constexpr size_t factor = 3;
    target->prepareForSquashing(source, factor);

    ASSERT_EQ(source->size(), 2u);
    const auto & target_qbit = assert_cast<const ColumnQBit &>(*target);
    ASSERT_GE(target_qbit.capacity(), source->size() * factor);
    for (const auto & tuple_column : target_qbit.getNestedData().getColumns())
        ASSERT_GE(tuple_column->capacity(), source->size() * factor);

    for (size_t batch = 0; batch != factor; ++batch)
        target->insertRangeFrom(*source, 0, source->size());

    ASSERT_EQ(target->size(), source->size() * factor);
    for (size_t i = 0; i != target->size(); ++i)
        ASSERT_EQ((*target)[i], (*source)[i % source->size()]);
}
