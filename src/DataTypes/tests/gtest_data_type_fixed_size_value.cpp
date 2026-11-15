#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>

#include <array>

using namespace DB;

/// A type whose size throws must not claim a fixed-size representation.
TEST(DataTypes, FixedSizeValuesHaveSize)
{
    /// Otherwise we won't have aggregate functions in factory.
    tryRegisterAggregateFunctions();

    const auto & factory = DataTypeFactory::instance();

    /// Argument shapes that between them instantiate every registered family.
    /// A new family that none of them fits fails the test until a shape for it is added.
    static constexpr std::array shapes
        = {"", "(3)", "(10, 2)", "(UInt8)", "(UInt8, UInt8)", "('a' = 1)", "(a UInt8)", "(sum, UInt64)", "(Float32, 8)"};

    for (const auto & name : factory.getAllRegisteredNames())
    {
        if (factory.isAlias(name))
            continue;

        DataTypePtr type;
        for (const auto * shape : shapes)
            if ((type = factory.tryGet(name + shape)))
                break;
        EXPECT_TRUE(type) << "no argument shape instantiates " << name;
        if (!type)
            continue;

        if (type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
            EXPECT_NO_THROW(type->getSizeOfValueInMemory())
                << "isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion() is true for " << type->getName()
                << ", so its getSizeOfValueInMemory() has to return the size instead of throwing";
    }
}
