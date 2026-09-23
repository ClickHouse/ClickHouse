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
#include <Common/CurrentThread.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/Defines.h>
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
    mutable size_t subcolumn_enumerations = 0;

    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override
    {
        ++enumerations;
        if (settings.subcolumn_name)
            ++subcolumn_enumerations;
        SerializationNumber<UInt64>::enumerateStreams(settings, callback, data);
    }
};

ColumnPtr parseJSON(
    const DataTypePtr & type,
    const SerializationPtr & serialization,
    std::string_view object,
    const FormatSettings & settings)
{
    auto column = type->createColumn();
    ReadBufferFromString input(object);
    serialization->deserializeWholeText(*column, input, settings);
    return column;
}

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
        serialization->subcolumn_enumerations = 0;
        EXPECT_NE(type->getSubcolumnType(prefix + "a.b"), nullptr);
        /// Both `a` and `a.b` can match; only `unrelated` must be skipped.
        EXPECT_EQ(serialization->subcolumn_enumerations, 2);

        serialization->subcolumn_enumerations = 0;
        EXPECT_NE(type->getSubcolumnType(prefix + "dynamic"), nullptr);
        EXPECT_EQ(serialization->subcolumn_enumerations, 0);

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

TEST(SerializationJSON, ValidatesSchemasWhenParsingText)
{
    tryRegisterAggregateFunctions();
    auto & factory = DataTypeFactory::instance();
    FormatSettings settings;
    for (const auto * schema : {"JSON(x Map(UInt64, String))", "JSON(x Array(Map(UInt64, String)))",
             "JSON(x Tuple(a Map(UInt64, String)))", "JSON(x AggregateFunction(sum, UInt64))"})
    {
        auto type = factory.get(schema);
        auto serialization = type->getDefaultSerialization();
        EXPECT_THROW(parseJSON(type, serialization, "{}", settings), Exception);
    }
    auto type = factory.get("JSON(x Array(Map(String, UInt64)), y Nullable(DateTime), z LowCardinality(String))");
    EXPECT_NO_THROW(parseJSON(type, type->getDefaultSerialization(), "{}", settings));
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
        workers.push_back(std::async(std::launch::async, [&]
        {
            const FormatSettings & worker_settings = settings;
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

TEST(SerializationJSON, ParsingUsesFormatSettingsTimezoneWithoutQueryScope)
{
    std::async(std::launch::async, []
    {
        ThreadStatus thread_status;
        ASSERT_EQ(CurrentThread::tryGetQueryContext(), nullptr);

        const auto & thread_timezone = DateLUT::instance();
        const auto & format_timezone = DateLUT::instance(
            thread_timezone.getTimeZone() == "Asia/Tokyo" ? "UTC" : "Asia/Tokyo");
        ASSERT_NE(&thread_timezone, &format_timezone);

        auto type = DataTypeFactory::instance().get(
            "JSON(d DateTime, d64 DateTime64(3), t Time, t64 Time64(3), fixed DateTime('UTC'))");
        auto serialization = type->getDefaultSerialization();
        FormatSettings settings;
        settings.json.session_timezone = &format_timezone;
        for (bool allow_simdjson : {false, true})
        {
            settings.json.allow_simdjson = allow_simdjson;
            auto column = parseJSON(
                type,
                serialization,
                R"({"d":"2024-01-01 12:00:00","d64":"2024-01-01 12:00:00.123","t":"12:34:56","t64":"12:34:56.123","fixed":"2024-01-01 12:00:00"})",
                settings);
            EXPECT_EQ(type->getSubcolumn("d", column)->getUInt(0), format_timezone.makeDateTime(2024, 1, 1, 12, 0, 0));
            EXPECT_EQ(type->getSubcolumn("d64", column)->getInt(0), format_timezone.makeDateTime(2024, 1, 1, 12, 0, 0) * 1000 + 123);
            EXPECT_EQ(type->getSubcolumn("t", column)->getInt(0), format_timezone.makeTime(12, 34, 56));
            EXPECT_EQ(type->getSubcolumn("t64", column)->getInt(0), format_timezone.makeTime(12, 34, 56) * 1000 + 123);
            EXPECT_EQ(type->getSubcolumn("fixed", column)->getUInt(0), DateLUT::instance("UTC").makeDateTime(2024, 1, 1, 12, 0, 0));

            WriteBufferFromOwnString output;
            serialization->serializeTextJSON(*column, 0, output, settings);
            EXPECT_NE(output.str().find(R"("d":"2024-01-01 12:00:00")"), String::npos);
            EXPECT_NE(output.str().find(R"("d64":"2024-01-01 12:00:00.123")"), String::npos);
            EXPECT_NE(output.str().find(R"("fixed":"2024-01-01 12:00:00")"), String::npos);

            auto dynamic_type = DataTypeFactory::instance().get("JSON(max_dynamic_paths = 0)");
            auto dynamic_serialization = dynamic_type->getDefaultSerialization();
            auto dynamic_column = parseJSON(dynamic_type, dynamic_serialization, R"({"d":"2024-01-01 12:00:00"})", settings);
            WriteBufferFromOwnString dynamic_output;
            dynamic_serialization->serializeTextJSON(*dynamic_column, 0, dynamic_output, settings);
            EXPECT_NE(dynamic_output.str().find(R"("d":"2024-01-01 12:00:00")"), String::npos);
        }
    }).get();
}

TEST(SerializationJSON, ParserCacheEvictsAfterMaximumSchemas)
{
    std::async(std::launch::async, []
    {
        ThreadStatus thread_status;
        FormatSettings settings;
        settings.json.allow_simdjson = false;
        settings.json.session_timezone = &DateLUT::instance("UTC");
        auto child_type = DataTypeFactory::instance().get("UInt64");
        std::weak_ptr<const ISerialization> first_serialization;

        for (size_t schema = 0; schema <= 64; ++schema)
        {
            auto type = std::make_shared<DataTypeObject>(
                DataTypeObject::SchemaFormat::JSON,
                std::unordered_map<String, DataTypePtr>{{"cache_limit_" + std::to_string(schema), child_type}});
            auto serialization = type->getDefaultSerialization();
            if (schema == 0)
                first_serialization = serialization;
            parseJSON(type, serialization, "{}", settings);
            if (schema == 0)
                EXPECT_FALSE(first_serialization.expired());
        }

        EXPECT_TRUE(first_serialization.expired());
    }).get();
}

TEST(SerializationJSON, ParserCacheReleasesOversizedObject)
{
    std::async(std::launch::async, []
    {
        ThreadStatus thread_status;
        FormatSettings settings;
        settings.json.allow_simdjson = false;
        settings.json.session_timezone = &DateLUT::instance("UTC");
        std::weak_ptr<const ISerialization> serialization_lifetime;
        {
            auto type = DataTypeFactory::instance().get("JSON(value String)");
            auto serialization = type->getDefaultSerialization();
            serialization_lifetime = serialization;
            String object = R"({"value":")";
            object.append(DBMS_DEFAULT_BUFFER_SIZE, 'x');
            object += R"("})";
            parseJSON(type, serialization, object, settings);
        }

        EXPECT_TRUE(serialization_lifetime.expired());
    }).get();
}
