#include <gtest/gtest.h>

#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>

/// A subcolumn's serialization is rebuilt from the subcolumn creators along its path, so it has to
/// reproduce the container path elements the whole column wrote. A dropped element is invisible while
/// `NullableElements` renders to nothing, and reads the wrong file as soon as it does not.

using namespace DB;

namespace
{

/// Every file name a subcolumn reads, rendered with namespaces so that the `Nullable` container is
/// visible in the name.
std::vector<String> namespacedStreamsOfSubcolumn(const DataTypePtr & type, const String & subcolumn_name)
{
    ISerialization::StreamFileNameSettings settings;
    settings.substream_naming_version = MergeTreeSubstreamNamingVersion::NAMESPACED;

    auto subcolumn_type = type->getSubcolumnType(subcolumn_name);
    auto subcolumn_serialization = type->getSubcolumnSerialization(subcolumn_name, type->getDefaultSerialization());

    std::vector<String> names;
    subcolumn_serialization->enumerateStreams(
        [&](const ISerialization::SubstreamPath & path) { names.push_back(ISerialization::getFileNameForStream("c", path, settings)); },
        subcolumn_type,
        nullptr);
    return names;
}

}

TEST(NullableSubcolumnStreams, SubcolumnsExtractedFromNullableStayInsideItsNamespace)
{
    /// `a` and `m` cannot be wrapped in `Nullable`, so they take the fall-through branch of
    /// `NullableSubcolumnCreator`; `a.b` and `m.size0` become `Nullable(...)` and take another. Streams
    /// of one column must not end up in different namespaces depending on which branch was taken.
    auto type = DataTypeFactory::instance().get("Nullable(JSON(`a` Tuple(b Int64), `m` Map(String, Int64)))");

    for (const auto & subcolumn_name : {"a", "a.b", "m", "m.size0", "m.keys", "m.keys.size", "m.values"})
    {
        auto names = namespacedStreamsOfSubcolumn(type, subcolumn_name);
        EXPECT_FALSE(names.empty()) << "no streams for subcolumn " << subcolumn_name;
        for (const auto & name : names)
        {
            /// The parent null map is the one stream that is a sibling of the namespace rather than
            /// inside it: the subcolumns that carry NULL themselves read it to fold it in.
            if (name == "c.null")
                continue;
            EXPECT_TRUE(name.starts_with("c.null_elems."))
                << "subcolumn " << subcolumn_name << " reads " << name << ", which is outside the Nullable namespace";
        }
    }
}

TEST(NullableSubcolumnStreams, SubcolumnStreamsAreASubsetOfTheWholeColumnStreams)
{
    /// The invariant behind the test above: whatever a subcolumn reads, the whole column must have
    /// written under the same name.
    ISerialization::StreamFileNameSettings settings;
    settings.substream_naming_version = MergeTreeSubstreamNamingVersion::NAMESPACED;

    for (const auto & type_name :
         {"Nullable(JSON(`a` Tuple(b Int64), `m` Map(String, Int64)))",
          "Nullable(Tuple(u UInt64, s Nullable(String)))",
          "Nullable(JSON(`arr` Array(Int64)))"})
    {
        auto type = DataTypeFactory::instance().get(type_name);

        std::set<String> whole_column;
        type->getDefaultSerialization()->enumerateStreams(
            [&](const ISerialization::SubstreamPath & path)
            { whole_column.insert(ISerialization::getFileNameForStream("c", path, settings)); },
            type,
            nullptr);

        IDataType::forEachSubcolumn(
            [&](const auto &, const auto & subcolumn_name, const auto &)
            {
                for (const auto & name : namespacedStreamsOfSubcolumn(type, subcolumn_name))
                    EXPECT_TRUE(whole_column.contains(name)) << type_name << ": subcolumn " << subcolumn_name << " reads " << name
                                                             << ", which the whole column never wrote";
            },
            ISerialization::SubstreamData(type->getDefaultSerialization()).withType(type));
    }
}
