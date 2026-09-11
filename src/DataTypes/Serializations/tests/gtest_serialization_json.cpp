#include <Core/Block.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNumberBase.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFactory.h>
#include <Formats/NativeWriter.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/FunctionsComparison.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/QueryScope.h>
#include <Common/ThreadStatus.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Storages/HivePartitioningUtils.h>

#include <array>
#include <future>
#include <gtest/gtest.h>

using namespace DB;

extern template class DB::FunctionComparison<DB::EqualsOp, DB::NameEquals>;

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

TEST(SerializationJSON, ValidatesSchemasWhenConstructingSerialization)
{
    tryRegisterAggregateFunctions();
    auto & factory = DataTypeFactory::instance();
    for (const auto * schema : {"JSON(x Map(UInt64, String))", "JSON(x Array(Map(UInt64, String)))",
             "JSON(x Tuple(a Map(UInt64, String)))", "JSON(x AggregateFunction(sum, UInt64))"})
    {
        auto type = factory.get(schema);
        EXPECT_THROW(type->getDefaultSerialization(), Exception);
    }
    EXPECT_NO_THROW(factory.get("JSON(x Array(Map(String, UInt64)), y Nullable(DateTime), z LowCardinality(String))")->getDefaultSerialization());
}

TEST(SerializationJSON, ParsingSettingsOwnResources)
{
    ASSERT_NE(getContext().context, nullptr);
    std::weak_ptr<const ISerialization> weak_serialization;
    std::weak_ptr<const IDataType> weak_type;
    auto settings = std::make_unique<FormatSettings>();
    EXPECT_EQ(settings->json_parsing_state->pools, nullptr);
    {
        FormatSettings copy;
        EXPECT_EQ(copy.json_parsing_state->pools, nullptr);
        copy = *settings;
        EXPECT_EQ(settings->json_parsing_state->pools, nullptr);
        EXPECT_EQ(settings->json_parsing_state, copy.json_parsing_state);
        FormatSettings independent;
        EXPECT_NE(settings->json_parsing_state, independent.json_parsing_state);
        independent = copy;
        EXPECT_EQ(settings->json_parsing_state, independent.json_parsing_state);
        auto type = DataTypeFactory::instance().get("JSON(x UInt64)");
        auto serialization = type->getDefaultSerialization();
        weak_type = type;
        weak_serialization = serialization;
        auto column = type->createColumn();
        EXPECT_EQ(settings->json_parsing_state->pools, nullptr);
        ReadBufferFromString input(std::string_view(R"({"x":42,"nested":{"a":[1,2]}})"));
        serialization->deserializeWholeText(*column, input, copy);
        EXPECT_NE(settings->json_parsing_state->pools, nullptr);
    }
    EXPECT_TRUE(weak_type.expired());
    EXPECT_FALSE(weak_serialization.expired());
    settings.reset();
    EXPECT_TRUE(weak_serialization.expired());
}

TEST(SerializationJSON, InputReleasesResourcesWithPersistentSettings)
{
    tryRegisterFormats();
    auto context = Context::createCopy(getContext().context);
    context->setSetting("input_format_parallel_parsing", false);
    context->setSetting("input_format_skip_unknown_fields", false);
    std::optional<FormatSettings> settings{std::in_place};
    settings->skip_unknown_fields = true;

    for (const auto * path : {"x", "y"})
    {
        std::weak_ptr<const ISerialization> weak_serialization;
        {
            auto type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON,
                std::unordered_map<String, DataTypePtr>{{path, DataTypeFactory::instance().get("UInt64")}});
            weak_serialization = type->getDefaultSerialization();
            Block header{{type->createColumn(), type, "j"}};
            const String data = R"({"ignored":0,"j":{")" + String(path) + R"(":42}})";
            ReadBufferFromString input(data);
            auto format = FormatFactory::instance().getInput("JSONEachRow", input, header, context, 10, settings);
            auto chunk = format->read();
            ASSERT_EQ(chunk.getNumRows(), 1);
            EXPECT_EQ(type->getSubcolumn(path, chunk.getColumns()[0])->getUInt(0), 42);
            EXPECT_EQ(format->read().getNumRows(), 0);
            EXPECT_EQ(settings->json_parsing_state->pools, nullptr);
        }
        EXPECT_TRUE(weak_serialization.expired());
    }
}

TEST(SerializationJSON, HivePartitionParsingReleasesResourcesWithPersistentSettings)
{
    std::optional<FormatSettings> settings{std::in_place};
    std::weak_ptr<const ISerialization> weak_serialization;
    {
        auto type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON,
            std::unordered_map<String, DataTypePtr>{{"x", DataTypeFactory::instance().get("UInt64")}});
        weak_serialization = type->getDefaultSerialization();
        auto hive_settings = HivePartitioningUtils::buildHiveFormatSettings(settings, getContext().context);
        auto value = convertFieldToType(Field(String(R"({"x":42})")), *type, nullptr, hive_settings);
        EXPECT_FALSE(value.isNull());
        EXPECT_NE(hive_settings.json_parsing_state->pools, nullptr);
        EXPECT_EQ(settings->json_parsing_state->pools, nullptr);
    }
    EXPECT_TRUE(weak_serialization.expired());
}

TEST(SerializationJSON, NativeSchemaInferenceDoesNotPopulatePersistentSettings)
{
    tryRegisterFormats();
    auto type = DataTypeFactory::instance().get("JSON(x UInt64)");
    auto column = type->createColumn();
    column->insert(Field(Object{{"x", UInt64(42)}}));
    Block block{{std::move(column), type, "j"}};
    std::optional<FormatSettings> settings{std::in_place};
    settings->native.write_json_as_string = true;
    WriteBufferFromOwnString output;
    NativeWriter writer(output, 0, std::make_shared<const Block>(block.cloneEmpty()), settings);
    writer.write(block);
    writer.flush();
    ReadBufferFromString input(output.str());
    auto reader = FormatFactory::instance().getSchemaReader("Native", input, getContext().context, settings);
    auto schema = reader->readSchema();
    ASSERT_EQ(schema.size(), 1);
    EXPECT_EQ(schema.front().type->getName(), type->getName());
    EXPECT_EQ(settings->json_parsing_state->pools, nullptr);
}

TEST(SerializationJSON, CastBlocksReleaseResourcesWithPersistentSettings)
{
    const FunctionConvertSettings settings(getContext().context, FormatSettings::DateTimeOverflowBehavior::Ignore);
    for (size_t block = 0; block < 2; ++block)
    {
        std::weak_ptr<const ISerialization> weak_serialization;
        {
            auto type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON,
                std::unordered_map<String, DataTypePtr>{{"x", DataTypeFactory::instance().get("UInt64")}});
            weak_serialization = type->getDefaultSerialization();
            auto strings = ColumnString::create();
            strings->insert(Field(String(R"({"x":42})")));
            ColumnsWithTypeAndName arguments{{std::move(strings), std::make_shared<DataTypeString>(), "j"}};
            auto result = DB::detail::ConvertImplGenericFromString<true>::execute(arguments, type, nullptr, 1, settings);
            EXPECT_EQ(type->getSubcolumn("x", result)->getUInt(0), 42);
            EXPECT_EQ(settings.format_settings.json_parsing_state->pools, nullptr);
        }
        EXPECT_TRUE(weak_serialization.expired());
    }
}

TEST(SerializationJSON, ComparisonBlocksReleaseResourcesWithPersistentSettings)
{
    const ComparisonParams params;
    FunctionPtr function = std::make_shared<FunctionComparison<EqualsOp, NameEquals>>(params);
    for (size_t block = 0; block < 2; ++block)
    {
        std::weak_ptr<const ISerialization> weak_serialization;
        {
            auto type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON,
                std::unordered_map<String, DataTypePtr>{{"x", DataTypeFactory::instance().get("UInt64")}});
            weak_serialization = type->getDefaultSerialization();
            auto objects = type->createColumn();
            objects->insert(Field(Object{{"x", UInt64(42)}}));
            auto string_type = std::make_shared<DataTypeString>();
            auto constant = string_type->createColumnConst(1, Field(String(R"({"x":42})")));
            ColumnsWithTypeAndName arguments{{std::move(objects), type, "j"}, {std::move(constant), string_type, "s"}};
            auto result = function->executeImpl(arguments, std::make_shared<DataTypeUInt8>(), 1);
            EXPECT_EQ(result->getUInt(0), 1);
            EXPECT_EQ(params.format_settings.json_parsing_state->pools, nullptr);
        }
        EXPECT_TRUE(weak_serialization.expired());
    }
}

TEST(SerializationJSON, AlternatingParsingSettingsLifetimes)
{
    ASSERT_NE(getContext().context, nullptr);
    auto type = DataTypeFactory::instance().get("JSON(x UInt64)");
    auto serialization = type->getDefaultSerialization();
    for (size_t iteration = 0; iteration < 10; ++iteration)
    {
        std::array<FormatSettings, 2> settings;
        for (size_t row = 0; row < 3; ++row)
        {
            for (const auto & current_settings : settings)
            {
                auto column = type->createColumn();
                ReadBufferFromString input(std::string_view(R"({"x":42})"));
                serialization->deserializeWholeText(*column, input, current_settings);
                EXPECT_EQ(type->getSubcolumn("x", column->getPtr())->getUInt(0), 42);
            }
        }
    }
}

TEST(SerializationJSON, ConcurrentParsingAndBinaryStrings)
{
    ASSERT_NE(getContext().context, nullptr);
    FormatSettings settings;
    settings.json.try_infer_numbers_from_strings = false;
    EXPECT_EQ(settings.json_parsing_state->pools, nullptr);
    auto type = DataTypeFactory::instance().get("JSON(x UInt64, nested Array(JSON))");
    auto serialization = type->getDefaultSerialization();
    std::vector<std::future<void>> workers;
    for (size_t worker = 0; worker < 4; ++worker)
    {
        workers.push_back(std::async(std::launch::async, [&]
        {
            FormatSettings worker_settings = settings; // NOLINT(performance-unnecessary-copy-initialization) -- Test concurrent first copies.
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
                    serialization->deserializeWholeText(*column, input, worker_settings);
                }
                EXPECT_EQ(column->size(), 100);
                auto x = type->getSubcolumn("x", column->getPtr());
                EXPECT_EQ(x->getUInt(99), 42);

                WriteBufferFromOwnString output;
                FormatSettings binary_settings = worker_settings;
                EXPECT_EQ(binary_settings.json_parsing_state, worker_settings.json_parsing_state);
                binary_settings.binary.write_json_as_string = true;
                binary_settings.binary.read_json_as_string = true;
                serialization->serializeBinary(*column, 0, output, binary_settings);
                auto restored = type->createColumn();
                ReadBufferFromString input(output.str());
                serialization->deserializeBinary(*restored, input, binary_settings);
                EXPECT_EQ(restored->compareAt(0, 0, *column, 1), 0);

                ReadBufferFromString malformed(std::string_view("{bad json}"));
                EXPECT_THROW(serialization->deserializeWholeText(*restored, malformed, worker_settings), Exception);
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
