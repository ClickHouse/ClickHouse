#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

/// `DataTypeDynamic::doGetSerialization` only forwards the settings to the pooled object when
/// `propagate_types_serialization_versions_to_nested_types` is set, so a settings object without it
/// would make every assertion below compare one and the same default-settings serialization.
SerializationInfoSettings settingsWithMapVersion(MergeTreeMapSerializationVersion map_version)
{
    SerializationInfoSettings settings;
    settings.version = MergeTreeSerializationInfoVersion::WITH_TYPES;
    settings.propagate_types_serialization_versions_to_nested_types = true;
    settings.map_serialization_version = map_version;
    return settings;
}

}

/// `SerializationDynamic` objects are interned in a pool keyed only on `getHash`, with no key
/// equality check on a hit (`SerializationObjectPool::getOrCreate`). Every field that changes the
/// constructed object must therefore participate in that hash: `map_serialization_version` decides
/// whether a nested `Map` variant reads and writes the bucketed stream layout (`.buckets_info`,
/// per-bucket streams) or the plain `Array(Tuple)` one, so two `Dynamic` objects differing only in
/// it must not be interned together.
TEST(DynamicPooledSerialization, PoolKeyDistinguishesMapSerializationVersion)
{
    auto dynamic_type = DataTypeFactory::instance().get("Dynamic(max_types=128)");

    auto basic = settingsWithMapVersion(MergeTreeMapSerializationVersion::BASIC);
    auto buckets = settingsWithMapVersion(MergeTreeMapSerializationVersion::WITH_BUCKETS);

    EXPECT_NE(SerializationDynamic::getHash(128, basic), SerializationDynamic::getHash(128, buckets));

    /// The pool hands out distinct objects. Both are held at once: the pool keeps `weak_ptr`s, so
    /// comparing pointers of temporaries could compare a reused allocation.
    auto basic_serialization = dynamic_type->getSerialization(basic);
    auto buckets_serialization = dynamic_type->getSerialization(buckets);
    EXPECT_NE(basic_serialization.get(), buckets_serialization.get());

    /// Positive control: a field that is already hashed separates the objects, so the assertions
    /// above are not passing merely because this pool never interns anything.
    auto nullable_changed = basic;
    nullable_changed.nullable_serialization_version = MergeTreeNullableSerializationVersion::ALLOW_SPARSE;
    EXPECT_NE(SerializationDynamic::getHash(128, basic), SerializationDynamic::getHash(128, nullable_changed));
    EXPECT_NE(basic_serialization.get(), dynamic_type->getSerialization(nullable_changed).get());

    /// Negative control: equal settings still share one object, so the assertions above cannot be
    /// satisfied by disabling interning.
    EXPECT_EQ(basic_serialization.get(), dynamic_type->getSerialization(basic).get());
    EXPECT_EQ(buckets_serialization.get(), dynamic_type->getSerialization(buckets).get());
}
