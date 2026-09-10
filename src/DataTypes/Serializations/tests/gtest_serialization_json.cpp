#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNumberBase.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/QueryScope.h>
#include <Common/ThreadStatus.h>
#include <Interpreters/Context.h>

#include <future>
#include <gtest/gtest.h>

using namespace DB;

namespace
{

class CountingType : public DataTypeNumberBase<UInt64>
{
public:
    mutable size_t constructions = 0;
    bool equals(const IDataType & rhs) const override { return typeid(rhs) == typeid(*this); }
    SerializationPtr doGetSerialization(const SerializationInfoSettings &) const override
    {
        ++constructions;
        return SerializationNumber<UInt64>::create();
    }
};

class NonPoolableSerialization : public SerializationNumber<UInt64>
{
public:
    bool supportsPooling() const override { return false; }
};

class CountingSerialization : public NonPoolableSerialization
{
public:
    mutable size_t enumerations = 0;

    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override
    {
        ++enumerations;
        SerializationNumber<UInt64>::enumerateStreams(settings, callback, data);
    }
};

}

TEST(SerializationJSON, SubcolumnLookupSkipsUnrelatedTypedPaths)
{
    auto child = std::make_shared<CountingType>();
    auto serialization = std::make_shared<CountingSerialization>();
    child->setCustomization(std::make_unique<DataTypeCustomDesc>(DataTypeCustomNamePtr{}, serialization));
    auto object = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON,
        std::unordered_map<String, DataTypePtr>{{"a", child}, {"a.b", child}, {"unrelated", child}});

    for (const auto & [type, prefix] : std::vector<std::pair<DataTypePtr, String>>{
             {object, ""}, {std::make_shared<DataTypeArray>(object), ""},
             {std::make_shared<DataTypeTuple>(DataTypes{object}, Names{"j"}), "j."}})
    {
        serialization->enumerations = 0;
        EXPECT_NE(type->getSubcolumnType(prefix + "a.b"), nullptr);
        EXPECT_EQ(serialization->enumerations, 2);

        serialization->enumerations = 0;
        EXPECT_NE(type->getSubcolumnType(prefix + "dynamic"), nullptr);
        EXPECT_EQ(serialization->enumerations, 0);

        serialization->enumerations = 0;
        EXPECT_FALSE(type->getSubcolumnNames().empty());
        EXPECT_EQ(serialization->enumerations, 3);
    }
}

TEST(SerializationJSON, WarmConstructionAndBoundedLifetime)
{
    auto child = std::make_shared<CountingType>();
    auto type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON,
        std::unordered_map<String, DataTypePtr>{{"x", child}});
    auto first = type->getDefaultSerialization();
    EXPECT_EQ(child->constructions, 1);
    EXPECT_EQ(first, type->getDefaultSerialization());
    EXPECT_EQ(child->constructions, 1);

    SerializationInfoSettings settings;
    settings.choose_kind = true;
    auto nondefault = type->getSerialization(settings);
    EXPECT_EQ(child->constructions, 2);
    EXPECT_EQ(nondefault, type->getSerialization(settings));
    EXPECT_EQ(child->constructions, 2);
    EXPECT_NE(first, nondefault);

    std::weak_ptr<const ISerialization> evicted = nondefault;
    nondefault.reset();
    settings.compute_exact_num_defaults = true;
    auto other = type->getSerialization(settings);
    EXPECT_TRUE(evicted.expired());
    EXPECT_EQ(first, type->getDefaultSerialization());
    EXPECT_EQ(child->constructions, 3);

    std::weak_ptr<const IDataType> weak_type = type;
    std::weak_ptr<const ISerialization> weak_first = first;
    type.reset();
    EXPECT_TRUE(weak_type.expired());
    first.reset();
    EXPECT_TRUE(weak_first.expired());
}

TEST(SerializationJSON, EquivalentSchemasShareAndCustomChildrenDoNotPool)
{
    auto & factory = DataTypeFactory::instance();
    auto first = factory.get("JSON(b String, a UInt64, SKIP c, SKIP REGEXP 'd.*')");
    auto second = factory.get("JSON(a UInt64, b String, SKIP c, SKIP REGEXP 'd.*')");
    EXPECT_EQ(first->getDefaultSerialization(), second->getDefaultSerialization());
    EXPECT_TRUE(first->getDefaultSerialization()->supportsPooling());

    auto custom = std::make_shared<CountingType>();
    custom->setCustomization(std::make_unique<DataTypeCustomDesc>(DataTypeCustomNamePtr{}, std::make_shared<NonPoolableSerialization>()));
    auto type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON,
        std::unordered_map<String, DataTypePtr>{{"x", custom}});
    auto serialization = type->getDefaultSerialization();
    EXPECT_FALSE(serialization->supportsPooling());
    EXPECT_NE(serialization, type->getDefaultSerialization());
}

TEST(SerializationJSON, ValidatesSchemasBeforeParsing)
{
    auto & factory = DataTypeFactory::instance();
    for (const auto * schema : {"JSON(x Map(UInt64, String))", "JSON(x Array(Map(UInt64, String)))",
             "JSON(x Tuple(a Map(UInt64, String)))", "JSON(x AggregateFunction(sum, UInt64))"})
        EXPECT_THROW(factory.get(schema), Exception);
    EXPECT_NO_THROW(factory.get("JSON(x Array(Map(String, UInt64)), y Nullable(DateTime), z LowCardinality(String))"));
}

TEST(SerializationJSON, ParsingSettingsOwnResources)
{
    ASSERT_NE(getContext().context, nullptr);
    std::weak_ptr<const ISerialization> weak_serialization;
    std::weak_ptr<const IDataType> weak_type;
    auto settings = std::make_unique<FormatSettings>();
    {
        FormatSettings independent;
        EXPECT_NE(settings->json_parsing_state, independent.json_parsing_state);
        FormatSettings copy = *settings;
        EXPECT_EQ(settings->json_parsing_state, copy.json_parsing_state);
        auto type = DataTypeFactory::instance().get("JSON(x UInt64)");
        auto serialization = type->getDefaultSerialization();
        weak_type = type;
        weak_serialization = serialization;
        auto column = type->createColumn();
        ReadBufferFromString input(std::string_view(R"({"x":42,"nested":{"a":[1,2]}})"));
        serialization->deserializeWholeText(*column, input, copy);
    }
    EXPECT_TRUE(weak_type.expired());
    EXPECT_FALSE(weak_serialization.expired());
    settings.reset();
    EXPECT_TRUE(weak_serialization.expired());
}

TEST(SerializationJSON, ConcurrentParsingAndBinaryStrings)
{
    ASSERT_NE(getContext().context, nullptr);
    FormatSettings settings;
    settings.json.try_infer_numbers_from_strings = false;
    auto type = DataTypeFactory::instance().get("JSON(x UInt64, nested Array(JSON))");
    auto serialization = type->getDefaultSerialization();
    std::vector<std::future<void>> workers;
    for (size_t worker = 0; worker < 4; ++worker)
    {
        workers.push_back(std::async(std::launch::async, [&, settings]
        {
            ThreadStatus thread_status;
            auto context = Context::createCopy(getContext().context);
            context->makeQueryContext();
            auto scope = QueryScope::create(context);
            for (bool simdjson : {false, true})
            {
                context->setSetting("allow_simdjson", simdjson);
                auto column = type->createColumn();
                for (size_t row = 0; row < 100; ++row)
                {
                    ReadBufferFromString input(std::string_view(R"({"x":42,"nested":[{"a":1}],"d":"2024-01-01 12:00:00"})"));
                    serialization->deserializeWholeText(*column, input, settings);
                }
                EXPECT_EQ(column->size(), 100);
                auto x = type->getSubcolumn("x", column->getPtr());
                EXPECT_EQ(x->getUInt(99), 42);

                WriteBufferFromOwnString output;
                FormatSettings binary_settings = settings;
                binary_settings.binary.write_json_as_string = true;
                binary_settings.binary.read_json_as_string = true;
                serialization->serializeBinary(*column, 0, output, binary_settings);
                auto restored = type->createColumn();
                ReadBufferFromString input(output.str());
                serialization->deserializeBinary(*restored, input, binary_settings);
                EXPECT_EQ(restored->compareAt(0, 0, *column, 1), 0);

                ReadBufferFromString malformed(std::string_view("{bad json}"));
                EXPECT_THROW(serialization->deserializeWholeText(*restored, malformed, settings), Exception);
                EXPECT_EQ(restored->size(), 1);
                ReadBufferFromString valid(output.str());
                serialization->deserializeBinary(*restored, valid, binary_settings);
                EXPECT_EQ(restored->size(), 2);
            }
        }));
    }
    for (auto & worker : workers)
        worker.get();
}
