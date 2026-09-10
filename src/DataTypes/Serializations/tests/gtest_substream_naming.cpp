#include <gtest/gtest.h>

#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/Serializations/ISerialization.h>

/// Renderer level tests for stream names. Paths are built by hand because the `Nullable(Array(...))`
/// shapes that `.nullN` exists for are not expressible yet; coverage over real types is functional.

using namespace DB;

namespace
{

using Substream = ISerialization::Substream;
using SubstreamPath = ISerialization::SubstreamPath;

/// `SubstreamPath` derives from `std::vector` without declaring constructors, so a braced list would
/// try to initialize the base from its first element.
SubstreamPath makePath(std::initializer_list<Substream> substreams)
{
    SubstreamPath path;
    path.assign(substreams.begin(), substreams.end());
    return path;
}

Substream tupleElement(const String & name)
{
    Substream substream(Substream::TupleElement);
    substream.name_of_substream = name;
    return substream;
}

Substream objectPath(const String & name)
{
    Substream substream(Substream::ObjectTypedPath);
    substream.object_path_name = name;
    return substream;
}

Substream namedStream(Substream::Type type, const String & name)
{
    Substream substream(type);
    substream.name_of_substream = name;
    return substream;
}

String subcolumnName(const SubstreamPath & path)
{
    return ISerialization::getSubcolumnNameForStream(path);
}

String basicFileName(const SubstreamPath & path)
{
    return ISerialization::getFileNameForStream("c", path, {});
}

String namespacedFileName(const SubstreamPath & path)
{
    ISerialization::StreamFileNameSettings settings;
    settings.substream_naming_version = MergeTreeSubstreamNamingVersion::NAMESPACED;
    return ISerialization::getFileNameForStream("c", path, settings);
}

}

/// A null map is numbered only when nothing separates it from the null maps above it.
TEST(SubstreamNaming, NullMapWithoutEnclosingNullableIsNotNumbered)
{
    /// Nullable(T)
    EXPECT_EQ(subcolumnName(makePath({Substream::NullMap})), "null");

    /// Array(Nullable(T)) - array elements emit nothing, but they are not a `Nullable` level either.
    EXPECT_EQ(subcolumnName(makePath({Substream::ArrayElements, Substream::NullMap})), "null");

    /// Array(Array(Nullable(T)))
    EXPECT_EQ(subcolumnName(makePath({Substream::ArrayElements, Substream::ArrayElements, Substream::NullMap})), "null");

    /// Nullable(Tuple(`a` Nullable(T))) - the tuple element name separates the two null maps.
    EXPECT_EQ(subcolumnName(makePath({Substream::NullableElements, tupleElement("a"), Substream::NullMap})), "a.null");

    /// Nullable(JSON(`a` Nullable(T))) - likewise for a JSON path name.
    EXPECT_EQ(
        subcolumnName(makePath({Substream::NullableElements, Substream::ObjectPaths, objectPath("a"), Substream::NullMap})), "a.null");
}

/// Shapes that only become expressible together with `Nullable(Array(...))`.
TEST(SubstreamNaming, NullMapIsNumberedPerNullableLevelSinceLastComponent)
{
    /// Nullable(Array(Nullable(T))): .null, .size0, .null1
    EXPECT_EQ(subcolumnName(makePath({Substream::NullMap})), "null");
    EXPECT_EQ(subcolumnName(makePath({Substream::NullableElements, Substream::ArraySizes})), "size0");
    EXPECT_EQ(subcolumnName(makePath({Substream::NullableElements, Substream::ArrayElements, Substream::NullMap})), "null1");

    /// Nullable(Array(Nullable(Array(Nullable(T))))): .null, .size0, .null1, .size1, .null2
    const auto inner_sizes
        = makePath({Substream::NullableElements, Substream::ArrayElements, Substream::NullableElements, Substream::ArraySizes});
    const auto inner_null_map = makePath(
        {Substream::NullableElements,
         Substream::ArrayElements,
         Substream::NullableElements,
         Substream::ArrayElements,
         Substream::NullMap});
    EXPECT_EQ(subcolumnName(inner_sizes), "size1");
    EXPECT_EQ(subcolumnName(inner_null_map), "null2");

    /// Array(Nullable(Array(Nullable(T)))): .size0, .null, .size1, .null1
    EXPECT_EQ(subcolumnName(makePath({Substream::ArraySizes})), "size0");
    EXPECT_EQ(subcolumnName(makePath({Substream::ArrayElements, Substream::NullMap})), "null");
    EXPECT_EQ(subcolumnName(makePath({Substream::ArrayElements, Substream::NullableElements, Substream::ArraySizes})), "size1");
    EXPECT_EQ(
        subcolumnName(
            makePath({Substream::ArrayElements, Substream::NullableElements, Substream::ArrayElements, Substream::NullMap})),
        "null1");

    /// Under NAMESPACED every container emits a component, which resets the counter, so a file name
    /// never carries the number: the namespace already disambiguates it.
    EXPECT_EQ(basicFileName(inner_null_map), "c.null2");
    EXPECT_EQ(namespacedFileName(inner_null_map), "c.null_elems.arr_elems.null_elems.arr_elems.null");
}

/// Nullable(Array(Nullable(Array(Nullable(Tuple(`a` Nullable(Array(Nullable(Array(UInt64))))))))))
/// Every null map in the chain must be reachable; the tuple element opens a fresh namespace.
TEST(SubstreamNaming, EveryNullMapOfANullableArrayChainIsReachable)
{
    const std::initializer_list<Substream> to_tuple{
        Substream::NullableElements,
        Substream::ArrayElements,
        Substream::NullableElements,
        Substream::ArrayElements,
        Substream::NullableElements};

    auto under_tuple = [&](std::initializer_list<Substream> rest)
    {
        SubstreamPath path;
        path.assign(to_tuple.begin(), to_tuple.end());
        path.push_back(tupleElement("a"));
        path.insert(path.end(), rest.begin(), rest.end());
        return path;
    };

    EXPECT_EQ(subcolumnName(makePath({Substream::NullMap})), "null");
    EXPECT_EQ(subcolumnName(makePath({Substream::NullableElements, Substream::ArrayElements, Substream::NullMap})), "null1");
    EXPECT_EQ(
        subcolumnName(makePath(
            {Substream::NullableElements,
             Substream::ArrayElements,
             Substream::NullableElements,
             Substream::ArrayElements,
             Substream::NullMap})),
        "null2");
    EXPECT_EQ(subcolumnName(under_tuple({})), "a");
    EXPECT_EQ(subcolumnName(under_tuple({Substream::NullMap})), "a.null");
    EXPECT_EQ(
        subcolumnName(under_tuple({Substream::NullableElements, Substream::ArrayElements, Substream::NullMap})), "a.null1");
}

/// These are the same streams seen through a standalone subcolumn serialization, so rendering them
/// from the path is what keeps a subcolumn read and a whole column read on the same file.
TEST(SubstreamNaming, NamedStreamsRenderFromPathAndIgnoreStampedName)
{
    const auto array_sizes = makePath({Substream::ArrayElements, Substream::ArraySizes});
    const auto named_offsets = makePath({Substream::ArrayElements, namedStream(Substream::NamedOffsets, "stamp_is_ignored")});

    EXPECT_EQ(subcolumnName(array_sizes), "size1");
    EXPECT_EQ(subcolumnName(named_offsets), subcolumnName(array_sizes));
    EXPECT_EQ(basicFileName(named_offsets), basicFileName(array_sizes));
    EXPECT_EQ(namespacedFileName(named_offsets), namespacedFileName(array_sizes));

    const auto null_map = makePath({Substream::NullableElements, Substream::ArrayElements, Substream::NullMap});
    const auto named_null_map
        = makePath({Substream::NullableElements, Substream::ArrayElements, namedStream(Substream::NamedNullMap, "stamp_is_ignored")});

    EXPECT_EQ(subcolumnName(null_map), "null1");
    EXPECT_EQ(subcolumnName(named_null_map), subcolumnName(null_map));
    EXPECT_EQ(basicFileName(named_null_map), basicFileName(null_map));
    EXPECT_EQ(namespacedFileName(named_null_map), namespacedFileName(null_map));
}

/// The collisions the namespaces exist to remove: under BASIC two streams of one column render to a
/// single file name, which is what corrupts the part.
TEST(SubstreamNaming, NamespacedSeparatesStreamsThatCollideUnderBasic)
{
    /// Array(Tuple(`size0` UInt64)): array sizes against a tuple element claiming their name. The
    /// files already differ under BASIC; the subcolumn names collide under both schemes.
    const auto array_sizes = makePath({Substream::ArraySizes});
    const auto shadowing_element = makePath({Substream::ArrayElements, tupleElement("size0")});
    EXPECT_EQ(subcolumnName(array_sizes), subcolumnName(shadowing_element));
    EXPECT_EQ(basicFileName(array_sizes), "c.size0");
    EXPECT_EQ(basicFileName(shadowing_element), "c%2Esize0");
    EXPECT_EQ(namespacedFileName(array_sizes), "c.size");
    EXPECT_EQ(namespacedFileName(shadowing_element), "c.arr_elems.size0");

    /// Tuple(`a` Tuple(`b` UInt64), `a.b` UInt64): one file for two streams under BASIC.
    const auto nested_element = makePath({tupleElement("a"), tupleElement("b")});
    const auto dotted_element = makePath({tupleElement("a.b")});
    EXPECT_EQ(basicFileName(nested_element), basicFileName(dotted_element));
    EXPECT_EQ(namespacedFileName(nested_element), "c.a.b");
    EXPECT_EQ(namespacedFileName(dotted_element), "c.a%2Eb");

    /// JSON(`a` Tuple(`b` UInt64), `a.b` UInt64): the same collision through JSON paths.
    const auto nested_path = makePath({Substream::ObjectPaths, objectPath("a"), tupleElement("b")});
    const auto dotted_path = makePath({Substream::ObjectPaths, objectPath("a.b")});
    EXPECT_EQ(basicFileName(nested_path), basicFileName(dotted_path));
    EXPECT_EQ(namespacedFileName(nested_path), "c.object_paths.a.b");
    EXPECT_EQ(namespacedFileName(dotted_path), "c.object_paths.a%2Eb");

    /// JSON(`object_structure` Int64): a declared path against an automatic stream of the same name.
    const auto object_structure = makePath({Substream::ObjectStructure});
    const auto shadowing_path = makePath({Substream::ObjectPaths, objectPath("object_structure")});
    EXPECT_EQ(basicFileName(object_structure), basicFileName(shadowing_path));
    EXPECT_EQ(namespacedFileName(object_structure), "c.object_structure");
    EXPECT_EQ(namespacedFileName(shadowing_path), "c.object_paths.object_structure");
}

/// Subcolumn names are frozen for compatibility: no namespace, and the array level is kept.
TEST(SubstreamNaming, SubcolumnNamesAreUnaffectedByNamespaces)
{
    const auto string_sizes = makePath({Substream::ArrayElements, Substream::ArrayElements, Substream::StringSizes});
    EXPECT_EQ(subcolumnName(string_sizes), "size");
    EXPECT_EQ(basicFileName(string_sizes), "c.size");
    EXPECT_EQ(namespacedFileName(string_sizes), "c.arr_elems.arr_elems.size");

    const auto map_keys = makePath({Substream::ArrayElements, tupleElement("keys")});
    EXPECT_EQ(subcolumnName(map_keys), "keys");
    EXPECT_EQ(basicFileName(map_keys), "c%2Ekeys");
    EXPECT_EQ(namespacedFileName(map_keys), "c.arr_elems.keys");
}
