#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationSparse.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(SparseSerialization, OffsetsStreamHasNoType)
{
    auto type = std::make_shared<DataTypeUInt64>();
    auto serialization = SerializationSparse::create(type->getDefaultSerialization());

    bool offsets_have_type = true;
    bool values_have_type = false;
    serialization->enumerateStreams(
        [&](const ISerialization::SubstreamPath & path)
        {
            if (path.back().type == ISerialization::Substream::SparseOffsets)
                offsets_have_type = path.back().data.type != nullptr;
            else if (path.back().type == ISerialization::Substream::Regular)
                values_have_type = path.back().data.type != nullptr;
        },
        type);

    EXPECT_FALSE(offsets_have_type);
    EXPECT_TRUE(values_have_type);
}
