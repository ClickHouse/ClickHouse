#include <Common/QueryFuzzer.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/CurrentMetrics.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromOStream.h>

#include <Access/Common/SQLSecurityDefs.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTRefreshStrategy.h>
#include <Parsers/ASTSQLSecurity.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTStatisticsDeclaration.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/SyncReplicaMode.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <pcg_random.hpp>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#if USE_BUZZHOUSE
#    include <Client/BuzzHouse/Generator/RandomGenerator.h>
namespace BuzzHouse
{
extern std::unordered_map<String, CHSetting> performanceSettings;
}
#endif

namespace CurrentMetrics
{
extern const Metric ASTFuzzerAccumulatedFragments;
}

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_DEEP_RECURSION;
}

void QueryFuzzer::getRandomSettings(SettingsChanges & settings_changes)
{
#if USE_BUZZHOUSE
    if (fuzz_rand() % 2 == 0)
    {
        const uint32_t nsettings
            = (fuzz_rand() % std::min(static_cast<uint32_t>(BuzzHouse::performanceSettings.size()), UINT32_C(20))) + UINT32_C(1);

        for (uint32_t i = 0; i < nsettings; i++)
        {
            const String & setting = pickRandomly(fuzz_rand, BuzzHouse::performanceSettings);
            String value = pickRandomly(fuzz_rand, BuzzHouse::performanceSettings.at(setting).oracle_values);

            value.erase(std::remove(value.begin(), value.end(), '\''), value.end());
            settings_changes.setSetting(setting, value);
        }
    }
#else
    UNUSED(settings_changes);
#endif
}

Field QueryFuzzer::getRandomField(int type)
{
    static constexpr Int64 bad_int64_values[]
        = {-2,
           -1,
           0,
           1,
           2,
           3,
           7,
           10,
           100,
           255,
           256,
           257,
           1023,
           1024,
           1025,
           65535,
           65536,
           65537,
           1024 * 1024 - 1,
           1024 * 1024,
           1024 * 1024 + 1,
           INT_MIN - 1ll,
           INT_MIN,
           INT_MIN + 1,
           INT_MAX - 1,
           INT_MAX,
           INT_MAX + 1ll,
           INT64_MIN,
           INT64_MIN + 1,
           INT64_MAX - 1,
           INT64_MAX};
    switch (type)
    {
        case 0: {
            return bad_int64_values[fuzz_rand() % std::size(bad_int64_values)];
        }
        case 1: {
            static constexpr double values[] = {NAN,       INFINITY,
                                                -INFINITY, 0.,
                                                -0.,       0.0001,
                                                0.5,       0.9999,
                                                1.,        1.0001,
                                                2.,        10.0001,
                                                100.0001,  1000.0001,
                                                1e10,      1e20,
                                                FLT_MIN,   FLT_MIN + FLT_EPSILON,
                                                FLT_MAX,   FLT_MAX + FLT_EPSILON};
            return values[fuzz_rand() % std::size(values)];
        }
        case 2: {
            static constexpr UInt64 scales[] = {0, 1, 2, 10};
            return DecimalField<Decimal64>(
                bad_int64_values[fuzz_rand() % std::size(bad_int64_values)], static_cast<UInt32>(scales[fuzz_rand() % std::size(scales)]));
        }
        case 3: {
            /// Date/Date32 boundary values as strings — stress date parsing and arithmetic
            static constexpr const char * date_values[]
                = {"0000-01-01", "1969-12-31", "1970-01-01", "2000-01-01", "2020-02-29", "2100-01-01", "2149-06-06", "9999-12-31"};
            return String(date_values[fuzz_rand() % std::size(date_values)]);
        }
        case 4: {
            /// Time/Time64 boundary values — stress time parsing, midnight wrap-around, sub-second precision and overflow
            static constexpr const char * time_values[]
                = {"00:00:00",
                   "00:00:00.000000000",
                   "23:59:59",
                   "23:59:59.999999999",
                   "-838:59:59",
                   "-838:59:59.999999999",
                   "838:59:59",
                   "838:59:59.999999999"};
            return String(time_values[fuzz_rand() % std::size(time_values)]);
        }
        case 5: {
            /// DateTime/DateTime64 boundary values as strings — stress timestamp parsing, overflow and sub-second precision
            static constexpr const char * datetime_values[]
                = {"1970-01-01 00:00:00",
                   "1970-01-01 00:00:00.000000000",
                   "2000-01-01 00:00:00",
                   "2020-02-29 23:59:59",
                   "2020-02-29 23:59:59.999999999",
                   "2038-01-19 03:14:07",
                   "2038-01-19 03:14:08",
                   "2106-02-07 06:28:15",
                   "9999-12-31 23:59:59",
                   "9999-12-31 23:59:59.999999999"};
            return String(datetime_values[fuzz_rand() % std::size(datetime_values)]);
        }
        case 6: {
            /// UUID boundary values — stress UUID parsing and comparison
            static constexpr const char * uuid_values[]
                = {"00000000-0000-0000-0000-000000000000",
                   "ffffffff-ffff-ffff-ffff-ffffffffffff",
                   "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                   "6ba7b811-9dad-11d1-80b4-00c04fd430c8"};
            return String(uuid_values[fuzz_rand() % std::size(uuid_values)]);
        }
        case 7: {
            /// IPv4 boundary values — stress network address parsing and arithmetic
            static constexpr const char * ipv4_values[] = {"0.0.0.0", "127.0.0.1", "192.168.0.1", "255.255.255.255", "10.0.0.1"};
            return String(ipv4_values[fuzz_rand() % std::size(ipv4_values)]);
        }
        case 8: {
            /// IPv6 boundary values — stress network address parsing and comparison
            static constexpr const char * ipv6_values[]
                = {"::", "::1", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "2001:db8::1", "::ffff:192.168.0.1", "fe80::1"};
            return String(ipv6_values[fuzz_rand() % std::size(ipv6_values)]);
        }
        case 9: {
            /// JSON string literals — stress JSONExtract*, JSON_VALUE, simpleJSON* and the JSON column type
            static constexpr const char * json_values[]
                = {"{}",
                   "[]",
                   "null",
                   R"({"a":1})",
                   R"({"a":null,"b":true,"c":false})",
                   R"({"a":{"b":{"c":42}}})",
                   R"({"a":[1,2,3],"b":"str"})",
                   R"([1,"two",null,true,{}])"};
            return String(json_values[fuzz_rand() % std::size(json_values)]);
        }
        default:
            assert(false);
            return Null{};
    }
}

Field QueryFuzzer::fuzzField(Field field)
{
    checkIterationLimit();

    const auto type = field.getType();

    int type_index = -1;

    if (type == Field::Types::Int64 || type == Field::Types::UInt64)
    {
        type_index = 0;
    }
    else if (type == Field::Types::Float64)
    {
        type_index = 1;
    }
    else if (
        type == Field::Types::Decimal32 || type == Field::Types::Decimal64 || type == Field::Types::Decimal128
        || type == Field::Types::Decimal256)
    {
        type_index = 2;
    }

    if (fuzz_rand() % 20 == 0)
    {
        return Null{};
    }

    if (type_index >= 0)
    {
        if (fuzz_rand() % 20 == 0)
        {
            // Change type sometimes, but not often, because it mostly leads to
            // boring errors.
            type_index = fuzz_rand() % 10;
        }
        return getRandomField(type_index);
    }

    if (type == Field::Types::String)
    {
        auto & str = field.safeGet<std::string>();
        const UInt64 action = fuzz_rand() % 12;
        switch (action)
        {
            case 0:
                str = "";
                break;
            case 1:
                str = str + str;
                break;
            case 2:
                str = str + str + str + str;
                break;
            case 4:
                if (!str.empty())
                {
                    str[fuzz_rand() % str.size()] = '\0';
                }
                break;
            case 5:
                /// Not UTF-8
                str = "\xF0\x28\x8C\xBC";
                break;
            case 6:
            case 7:
                /// For LIKE strings
                if (str.size() < 128)
                {
                    for (auto & c : str)
                    {
                        if ((c == '_' || c == '%') && ((fuzz_rand() % 2) == 0))
                        {
                            c = (c == '_') ? '%' : '_';
                        }
                    }
                }
                break;
            default:
                /// Do nothing
                break;
        }
    }
    else if (type == Field::Types::Array)
    {
        auto & arr = field.safeGet<Array>();

        if (fuzz_rand() % 5 == 0 && !arr.empty())
        {
            const size_t pos = fuzz_rand() % arr.size();
            arr.erase(arr.begin() + pos);
            if (debug_stream)
                *debug_stream << "erased\n";
        }

        if (fuzz_rand() % 5 == 0)
        {
            if (!arr.empty())
            {
                const size_t pos = fuzz_rand() % arr.size();
                arr.insert(arr.begin() + pos, fuzzField(arr[pos]));
                if (debug_stream)
                    *debug_stream << fmt::format("inserted (pos {})\n", pos);
            }
            else
            {
                arr.insert(arr.begin(), getRandomField(0));
                if (debug_stream)
                    *debug_stream << "inserted (0)\n";
            }
        }

        for (auto & element : arr)
        {
            element = fuzzField(element);
        }
    }
    else if (type == Field::Types::Tuple)
    {
        auto & arr = field.safeGet<Tuple>();

        if (fuzz_rand() % 5 == 0 && !arr.empty())
        {
            const size_t pos = fuzz_rand() % arr.size();
            arr.erase(arr.begin() + pos);

            if (debug_stream)
                *debug_stream << "erased\n";
        }

        if (fuzz_rand() % 5 == 0)
        {
            if (!arr.empty())
            {
                const size_t pos = fuzz_rand() % arr.size();
                arr.insert(arr.begin() + pos, fuzzField(arr[pos]));

                if (debug_stream)
                    *debug_stream << fmt::format("inserted (pos {})\n", pos);
            }
            else
            {
                arr.insert(arr.begin(), getRandomField(0));

                if (debug_stream)
                    *debug_stream << "inserted (0)\n";
            }
        }

        for (auto & element : arr)
        {
            element = fuzzField(element);
        }
    }

    return field;
}

ASTPtr QueryFuzzer::getRandomColumnLike()
{
    if (column_like.empty())
    {
        return nullptr;
    }

    ASTPtr new_ast = column_like[fuzz_rand() % column_like.size()].second->clone();
    return setIdentifierAliasOrNot(new_ast);
}

ASTPtr QueryFuzzer::getRandomExpressionList(const size_t nproj)
{
    if (column_like.empty())
    {
        return nullptr;
    }

    ASTPtr new_ast = make_intrusive<ASTExpressionList>();
    for (size_t i = 0; i < fuzz_rand() % 5 + 1; ++i)
    {
        /// Use Group by number in the projection, starting from position 1
        new_ast->children.emplace_back(
            nproj && (fuzz_rand() % 4 == 0) ? make_intrusive<ASTLiteral>((fuzz_rand() % nproj) + 1) : getRandomColumnLike());
    }
    return new_ast;
}

void QueryFuzzer::fuzzOrderByElement(ASTOrderByElement * elem)
{
    switch (fuzz_rand() % 10)
    {
        case 0:
            elem->direction = -1;
            break;
        case 1:
            elem->direction = 1;
            break;
        case 2:
            elem->nulls_direction = -1;
            elem->nulls_direction_was_explicitly_specified = true;
            break;
        case 3:
            elem->nulls_direction = 1;
            elem->nulls_direction_was_explicitly_specified = true;
            break;
        case 4:
            elem->nulls_direction = elem->direction;
            elem->nulls_direction_was_explicitly_specified = false;
            break;
        case 5:
            if (fuzz_rand() % 5 == 0)
                elem->with_fill = !elem->with_fill;
            break;
        default:
            // do nothing
            break;
    }
}

void QueryFuzzer::fuzzOrderByList(IAST * ast, const size_t nproj)
{
    if (!ast)
    {
        return;
    }

    checkIterationLimit();

    auto * list = assert_cast<ASTExpressionList *>(ast);

    /// Permute list
    if (!list->children.empty() && fuzz_rand() % 20 == 0)
    {
        std::shuffle(list->children.begin(), list->children.end(), fuzz_rand);
    }

    // Remove element
    if (fuzz_rand() % 50 == 0 && list->children.size() > 1)
    {
        // Don't remove last element -- this leads to questionable
        // constructs such as empty select.
        list->children.erase(list->children.begin() + fuzz_rand() % list->children.size());
    }

    // Add element
    if (fuzz_rand() % 50 == 0)
    {
        /// Order by one of the projections, starting from position 1
        auto pos = list->children.empty() ? list->children.begin() : list->children.begin() + fuzz_rand() % list->children.size();
        const auto col = nproj && (fuzz_rand() % 4 == 0) ? make_intrusive<ASTLiteral>((fuzz_rand() % nproj) + 1) : getRandomColumnLike();
        if (col)
        {
            auto elem = make_intrusive<ASTOrderByElement>();
            elem->children.emplace_back(col);
            elem->direction = 1;
            elem->nulls_direction = 1;
            elem->nulls_direction_was_explicitly_specified = false;
            elem->with_fill = false;

            list->children.insert(pos, elem);
        }
        else
        {
            if (debug_stream)
                *debug_stream << "No random column.\n";
        }
    }

    // We don't have to recurse here to fuzz the children, this is handled by
    // the generic recursion into IAST.children.
}

void QueryFuzzer::fuzzColumnLikeExpressionList(IAST * ast)
{
    if (!ast)
    {
        return;
    }

    checkIterationLimit();

    auto * impl = assert_cast<ASTExpressionList *>(ast);

    /// Permute list
    if (!impl->children.empty() && fuzz_rand() % 20 == 0)
    {
        std::shuffle(impl->children.begin(), impl->children.end(), fuzz_rand);
    }

    // Remove element
    if (fuzz_rand() % 50 == 0 && impl->children.size() > 1)
    {
        // Don't remove last element -- this leads to questionable
        // constructs such as empty select.
        impl->children.erase(impl->children.begin() + fuzz_rand() % impl->children.size());
    }

    // Add element
    if (fuzz_rand() % 50 == 0)
    {
        auto pos = impl->children.empty() ? impl->children.begin() : impl->children.begin() + fuzz_rand() % impl->children.size();
        auto col = getRandomColumnLike();
        if (col)
            impl->children.insert(pos, col);
        else if (debug_stream)
            *debug_stream << "No random column.\n";
    }

    // We don't have to recurse here to fuzz the children, this is handled by
    // the generic recursion into IAST.children.
}

NullsAction QueryFuzzer::fuzzNullsAction(NullsAction action)
{
    /// If it's not using actions, then it's a high change it doesn't support it to begin with
    if ((action == NullsAction::EMPTY) && (fuzz_rand() % 100 == 0))
    {
        if (fuzz_rand() % 2 == 0)
            return NullsAction::RESPECT_NULLS;
        else
            return NullsAction::IGNORE_NULLS;
    }
    else if (fuzz_rand() % 20 == 0)
    {
        switch (fuzz_rand() % 3)
        {
            case 0:
                return NullsAction::EMPTY;
            case 1:
                return NullsAction::RESPECT_NULLS;
            default:
                return NullsAction::IGNORE_NULLS;
        }
    }
    return action;
}

void QueryFuzzer::fuzzWindowDefinition(ASTWindowDefinition & def)
{
    auto removeChild = [](ASTWindowDefinition & wdef, ASTPtr & member)
    {
        auto & ch = wdef.children;
        member->children.clear();
        ch.erase(std::remove(ch.begin(), ch.end(), member), ch.end());
        member = nullptr;
    };

    if (def.partition_by)
    {
        if (fuzz_rand() % 50 == 0)
            removeChild(def, def.partition_by);
        else
            fuzzColumnLikeExpressionList(def.partition_by.get());
    }
    if (def.order_by)
    {
        if (fuzz_rand() % 50 == 0)
            removeChild(def, def.order_by);
        else
            fuzzOrderByList(def.order_by.get(), 0);
    }
    fuzzWindowFrame(def);
}

void QueryFuzzer::fuzzWindowFrame(ASTWindowDefinition & def)
{
    checkIterationLimit();

    switch (fuzz_rand() % 20)
    {
        case 0: {
            const auto r = fuzz_rand() % 3;
            def.frame_type = r == 0 ? WindowFrame::FrameType::ROWS
                : r == 1            ? WindowFrame::FrameType::RANGE
                                    : WindowFrame::FrameType::GROUPS;
            break;
        }
        case 1: {
            const auto r = fuzz_rand() % 3;
            def.frame_begin_type = r == 0 ? WindowFrame::BoundaryType::Unbounded
                : r == 1                  ? WindowFrame::BoundaryType::Current
                                          : WindowFrame::BoundaryType::Offset;

            if (def.frame_begin_type == WindowFrame::BoundaryType::Offset)
            {
                // The offsets are fuzzed normally through 'children'.
                def.frame_begin_offset = make_intrusive<ASTLiteral>(getRandomField(0));
            }
            else
            {
                def.frame_begin_offset = nullptr;
            }
            break;
        }
        case 2: {
            const auto r = fuzz_rand() % 3;
            def.frame_end_type = r == 0 ? WindowFrame::BoundaryType::Unbounded
                : r == 1                ? WindowFrame::BoundaryType::Current
                                        : WindowFrame::BoundaryType::Offset;

            if (def.frame_end_type == WindowFrame::BoundaryType::Offset)
            {
                def.frame_end_offset = make_intrusive<ASTLiteral>(getRandomField(0));
            }
            else
            {
                def.frame_end_offset = nullptr;
            }
            break;
        }
        case 3: {
            def.frame_begin_preceding = fuzz_rand() % 2;
            break;
        }
        case 4: {
            def.frame_end_preceding = fuzz_rand() % 2;
            break;
        }
        default:
            break;
    }

    if (def.frame_type == WindowFrame::FrameType::RANGE && def.frame_begin_type == WindowFrame::BoundaryType::Unbounded
        && def.frame_begin_preceding && def.frame_end_type == WindowFrame::BoundaryType::Current)
    {
        def.frame_is_default = true; /* NOLINT clang-tidy could you just shut up please */
    }
    else
    {
        def.frame_is_default = false;
    }
}

bool QueryFuzzer::isSuitableForFuzzing(const ASTCreateQuery & create)
{
    return create.columns_list && create.columns_list->columns;
}

static String getOriginalTableName(const String & full_name)
{
    return full_name.substr(0, full_name.find("__fuzz_"));
}

static String getFuzzedTableName(const String & original_name, size_t index)
{
    return original_name + "__fuzz_" + toString(index);
}

void QueryFuzzer::fuzzCreateQuery(ASTCreateQuery & create)
{
    if (create.columns_list && create.columns_list->columns)
    {
        for (auto & ast : create.columns_list->columns->children)
        {
            if (auto * column = ast->as<ASTColumnDeclaration>())
            {
                fuzzColumnDeclaration(*column);
            }
        }
    }

    if (create.columns_list && create.columns_list->indices)
    {
        for (auto & ast : create.columns_list->indices->children)
        {
            if (auto * index = ast->as<ASTIndexDeclaration>())
                fuzzIndexDeclaration(*index);
        }
    }

    if (create.storage && create.storage->engine)
    {
        auto & engine_name = create.storage->engine->name;

        if (create.database && !create.table)
        {
            /// For database engine fuzzing, only swap between parameter-free engines.
            /// Avoid touching Replicated (needs ZooKeeper), Lazy (needs arg), or external
            /// engines (MySQL/PostgreSQL need connection params).
            static const Strings safe_database_engines = {"Atomic", "Memory"};
            if ((engine_name == "Atomic" || engine_name == "Memory") && fuzz_rand() % 10 == 0)
            {
                engine_name = pickRandomly(fuzz_rand, safe_database_engines);
                if (auto & arguments = create.storage->engine->arguments)
                    arguments->children.clear();
            }
        }
        else
        {
            /// Replace ReplicatedMergeTree to ordinary MergeTree
            /// to avoid inconsistency of metadata in zookeeper.
            if (startsWith(engine_name, "Replicated"))
            {
                engine_name = engine_name.substr(strlen("Replicated"));
                if (auto & arguments = create.storage->engine->arguments)
                {
                    auto & children = arguments->children;
                    if (children.size() <= 2)
                        arguments.reset();
                    else
                        children.erase(children.begin(), children.begin() + 2);
                }
            }

            /// Swap between MergeTree variants that require no mandatory extra columns.
            /// CollapsingMergeTree/VersionedCollapsingMergeTree require a sign column and
            /// GraphiteMergeTree requires a config name, so those are excluded.
            if (endsWith(engine_name, "MergeTree") && fuzz_rand() % 20 == 0)
            {
                static const Strings safe_mergetree_engines = {
                    "MergeTree",
                    "AggregatingMergeTree",
                    "SummingMergeTree",
                    "ReplacingMergeTree",
                };
                engine_name = pickRandomly(fuzz_rand, safe_mergetree_engines);
                /// Clear engine arguments to avoid arity mismatches with the new engine
                if (auto & arguments = create.storage->engine->arguments)
                    arguments->children.clear();
            }
        }
    }

    /// For MergeTree family engines, inject hot table settings with low probability.
    if (create.storage && create.storage->engine && endsWith(create.storage->engine->name, "MergeTree"))
    {
        static const Strings hot_bool_settings
            = {"add_minmax_index_for_numeric_columns",
               "add_minmax_index_for_string_columns",
               "add_minmax_index_for_temporal_columns",
               "allow_coalescing_columns_in_partition_or_order_key",
               "allow_experimental_reverse_key",
               "allow_floating_point_partition_key",
               "allow_nullable_key",
               "allow_summing_columns_in_partition_or_order_key",
               "allow_suspicious_indices",
               "allow_vertical_merges_from_compact_to_wide_parts",
               "enable_block_number_column",
               "enable_block_offset_column",
               "enable_vertical_merge_algorithm",
               "ttl_only_drop_parts"};

        auto fuzz_setting = [&](const String & name, Field value)
        {
            if (!create.storage->settings)
            {
                auto new_settings = make_intrusive<ASTSetQuery>();
                new_settings->is_standalone = false;
                create.storage->set(create.storage->settings, new_settings);
            }
            create.storage->settings->changes.emplace_back(name, std::move(value));
        };

        for (const auto & name : hot_bool_settings)
            if (fuzz_rand() % 20 == 0)
                fuzz_setting(name, UInt64(1));

        if (fuzz_rand() % 20 == 0)
            fuzz_setting(
                "deduplicate_merge_projection_mode", String(pickRandomly(fuzz_rand, Strings{"ignore", "throw", "drop", "rebuild"})));
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("inactive_parts_to_delay_insert", UInt64(fuzz_rand() % 50 + 1));
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("index_granularity", UInt64(1) << (fuzz_rand() % 14));
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("lightweight_mutation_projection_mode", String(pickRandomly(fuzz_rand, Strings{"throw", "drop", "rebuild"})));
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("max_avg_part_size_for_too_many_parts", UInt64(fuzz_rand() % (1 << 24)));
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("merge_max_block_size", UInt64(1) << (fuzz_rand() % 14));
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("merge_with_ttl_timeout", Int64(fuzz_rand() % 7200));
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("min_bytes_for_full_part_storage", UInt64(1) << (fuzz_rand() % 14));
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("min_bytes_for_wide_part", UInt64(fuzz_rand() % 2));
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("nullable_serialization_version", String(fuzz_rand() % 2 == 0 ? "basic" : "allow_sparse"));
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("number_of_partitions_to_consider_for_merge", UInt64(fuzz_rand() % 50 + 1));
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("parts_to_delay_insert", UInt64(fuzz_rand() % 50 + 1));
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("ratio_of_defaults_for_sparse_serialization", Float64(fuzz_rand() % 101) / 100.0);
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("remove_empty_parts", UInt64(0));
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("string_serialization_version", String(fuzz_rand() % 2 == 0 ? "single_stream" : "with_size_stream"));
        if (fuzz_rand() % 20 == 0)
            fuzz_setting("vertical_merge_algorithm_min_bytes_to_activate", UInt64(1) << (fuzz_rand() % 14));
    }

    /// Fuzz CREATE MATERIALIZED VIEW: toggle POPULATE and refresh strategy parameters
    if (create.is_materialized_view)
    {
        if (fuzz_rand() % 20 == 0)
            create.is_populate = !create.is_populate;

        if (create.refresh_strategy)
        {
            /// Fuzz refresh period
            if (create.refresh_strategy->period && fuzz_rand() % 5 == 0)
                create.refresh_strategy->period->interval.seconds = static_cast<UInt64>(fuzz_rand() % 3600) + 1;

            /// Fuzz spread (jitter)
            if (create.refresh_strategy->spread && fuzz_rand() % 5 == 0)
                create.refresh_strategy->spread->interval.seconds = static_cast<UInt64>(fuzz_rand() % 300);

            /// Toggle APPEND
            if (fuzz_rand() % 10 == 0)
                create.refresh_strategy->append = !create.refresh_strategy->append;

            /// Toggle schedule kind between EVERY and AFTER
            if (create.refresh_strategy->schedule_kind != RefreshScheduleKind::UNKNOWN && fuzz_rand() % 10 == 0)
            {
                create.refresh_strategy->schedule_kind = (create.refresh_strategy->schedule_kind == RefreshScheduleKind::EVERY)
                    ? RefreshScheduleKind::AFTER
                    : RefreshScheduleKind::EVERY;
            }
        }
    }

    /// Fuzz SQL SECURITY type for ordinary and materialized views
    if (create.supportSQLSecurity() && create.sql_security && fuzz_rand() % 10 == 0)
    {
        auto * sec = create.sql_security->as<ASTSQLSecurity>();
        if (sec && sec->type)
        {
            static constexpr SQLSecurityType security_types[] = {SQLSecurityType::INVOKER, SQLSecurityType::DEFINER, SQLSecurityType::NONE};
            sec->type = security_types[fuzz_rand() % std::size(security_types)];
        }
    }

    /// Fuzz CREATE DICTIONARY: swap layout type and fuzz lifetime
    if (create.is_dictionary && create.dictionary)
    {
        /// Swap layout among parameter-free layout types
        if (create.dictionary->layout && fuzz_rand() % 5 == 0)
        {
            static const Strings simple_layouts = {"flat", "hashed", "sparse_hashed", "direct"};
            create.dictionary->layout->layout_type = simple_layouts[fuzz_rand() % simple_layouts.size()];
        }

        /// Fuzz lifetime bounds
        if (create.dictionary->lifetime && fuzz_rand() % 5 == 0)
        {
            const UInt64 new_max = static_cast<UInt64>(fuzz_rand() % 3600) + 1;
            const UInt64 new_min = fuzz_rand() % (new_max + 1);
            create.dictionary->lifetime->min_sec = new_min;
            create.dictionary->lifetime->max_sec = new_max;
        }
    }

    /// Fuzz dictionary attribute flags and default values
    if (create.is_dictionary && create.dictionary_attributes_list)
    {
        for (auto & child : create.dictionary_attributes_list->children)
        {
            auto * attr = child->as<ASTDictionaryAttributeDeclaration>();
            if (!attr)
                continue;

            /// Toggle injective: affects GROUP BY optimization
            if (fuzz_rand() % 10 == 0)
                attr->injective = !attr->injective;

            /// Toggle hierarchical: enables dictGetHierarchy / dictIsIn
            if (fuzz_rand() % 20 == 0)
                attr->hierarchical = !attr->hierarchical;

            /// Toggle bidirectional (only meaningful with hierarchical)
            if (attr->hierarchical && fuzz_rand() % 10 == 0)
                attr->bidirectional = !attr->bidirectional;

            /// Fuzz default value literal
            if (attr->default_value && fuzz_rand() % 5 == 0)
            {
                if (auto * lit = attr->default_value->as<ASTLiteral>())
                    lit->value = fuzzField(lit->value);
            }
        }
    }

    /// Toggle CREATE ↔ CREATE OR REPLACE
    if (fuzz_rand() % 100 == 0)
        create.create_or_replace = !create.create_or_replace;


    /// Drop storage clauses: each is optional and null-checked by formatImpl
    if (create.storage)
    {
        /// Helper: remove a raw-pointer child from storage->children and null it
        auto drop_storage_clause = [&](IAST *& ptr)
        {
            if (!ptr)
                return;
            auto & ch = create.storage->children;
            ch.erase(std::remove_if(ch.begin(), ch.end(), [&](const ASTPtr & c) { return c.get() == ptr; }), ch.end());
            ptr = nullptr;
        };

        if (fuzz_rand() % 50 == 0)
            drop_storage_clause(create.storage->sample_by);
        if (fuzz_rand() % 50 == 0)
            drop_storage_clause(create.storage->primary_key);
        if (fuzz_rand() % 50 == 0)
            drop_storage_clause(create.storage->ttl_table);
        /// PARTITION BY removal is rarer — changes table sharding fundamentally
        if (fuzz_rand() % 100 == 0)
            drop_storage_clause(create.storage->partition_by);
    }

    /// Fuzz or drop existing projections
    if (create.columns_list && create.columns_list->projections)
    {
        auto & projs = create.columns_list->projections->children;
        for (auto & proj_ast : projs)
            if (auto * proj = proj_ast->as<ASTProjectionDeclaration>())
                fuzzProjectionDeclaration(*proj);
        /// Drop a random projection (exercises projection removal path)
        if (!projs.empty() && fuzz_rand() % 50 == 0)
            projs.erase(projs.begin() + fuzz_rand() % projs.size());
    }

    /// Drop a random constraint (exercises constraint removal path)
    if (create.columns_list && create.columns_list->constraints && !create.columns_list->constraints->children.empty()
        && fuzz_rand() % 50 == 0)
    {
        auto & cons = create.columns_list->constraints->children;
        cons.erase(cons.begin() + fuzz_rand() % cons.size());
    }

    auto full_name = create.getTable();
    auto original_name = getOriginalTableName(full_name);
    size_t index = index_of_fuzzed_table[original_name]++;
    auto new_name = getFuzzedTableName(original_name, index);

    create.setTable(new_name);

    SipHash sip_hash;
    sip_hash.update(original_name);
    if (create.columns_list)
        create.columns_list->updateTreeHash(sip_hash, /*ignore_aliases=*/true);
    if (create.storage)
        create.storage->updateTreeHash(sip_hash, /*ignore_aliases=*/true);

    const auto hash = getSipHash128AsPair(sip_hash);

    /// Save only tables with unique definition.
    if (created_tables_hashes.insert(hash).second)
        original_table_name_to_fuzzed[original_name].insert(new_name);
}

void QueryFuzzer::fuzzColumnDeclaration(ASTColumnDeclaration & column)
{
    if (auto type = column.getType())
    {
        auto data_type = fuzzDataType(DataTypeFactory::instance().get(type));

        ParserDataType parser;
        column.setType(parseQuery(
            parser, data_type->getName(), DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS));
    }

    if (auto stats = column.getStatisticsDesc())
    {
        static const Strings stat_types = {"tdigest", "countmin", "minmax", "uniq"};
        auto * stats_decl = stats->as<ASTStatisticsDeclaration>();
        if (stats_decl && stats_decl->types)
        {
            for (auto & type_ast : stats_decl->types->children)
            {
                if (auto * fn = type_ast->as<ASTFunction>(); fn && fuzz_rand() % 5 == 0)
                    fn->name = pickRandomly(fuzz_rand, stat_types);
            }
        }
    }

    if (auto codec = column.getCodec())
    {
        auto * codec_fn = codec->as<ASTFunction>();
        if (codec_fn && codec_fn->name == "CODEC" && codec_fn->arguments && fuzz_rand() % 5 == 0)
        {
            codec_fn->arguments->children.clear();
            switch (fuzz_rand() % 4)
            {
                case 0:
                    codec_fn->arguments->children.push_back(makeASTFunction("NONE"));
                    break;
                case 1:
                    codec_fn->arguments->children.push_back(makeASTFunction("LZ4"));
                    break;
                case 2:
                    codec_fn->arguments->children.push_back(
                        makeASTFunction("ZSTD", make_intrusive<ASTLiteral>(UInt64(fuzz_rand() % 22 + 1))));
                    break;
                case 3:
                    codec_fn->arguments->children.push_back(
                        makeASTFunction("LZ4HC", make_intrusive<ASTLiteral>(UInt64(fuzz_rand() % 12 + 1))));
                    break;
                default:
                    /// Do nothing
                    break;
            }
        }
    }

    if (column.default_specifier != ColumnDefaultSpecifier::Empty && column.default_specifier != ColumnDefaultSpecifier::AutoIncrement
        && column.getDefaultExpression() && fuzz_rand() % 5 == 0)
    {
        switch (fuzz_rand() % 4)
        {
            case 0:
                column.default_specifier = ColumnDefaultSpecifier::Default;
                break;
            case 1:
                column.default_specifier = ColumnDefaultSpecifier::Materialized;
                break;
            case 2:
                column.default_specifier = ColumnDefaultSpecifier::Alias;
                break;
            case 3:
                column.default_specifier = ColumnDefaultSpecifier::Ephemeral;
                break;
            default:
                /// Do nothing
                break;
        }
    }
}

void QueryFuzzer::fuzzIndexDeclaration(ASTIndexDeclaration & index)
{
    auto index_type = index.getType();
    if (!index_type)
        return;

    /// No-arg index types: safe to swap to and clear any existing arguments.
    static const Strings simple_index_types = {"minmax", "set", "bloom_filter"};
    /// BF index types: require positional arguments — swap name only, keep args.
    static const std::unordered_set<String> bf_index_types = {"ngrambf_v1", "tokenbf_v1"};
    /// Simple no-arg tokenizers valid as text index tokenizer values.
    static const Strings simple_tokenizers = {"splitByNonAlpha", "splitByString", "array"};
    static const Strings posting_list_codecs = {"none", "bitpacking"};

    /// Fuzz named parameters of text index independently of type swap.
    if (index_type->name == "text" && index_type->arguments)
    {
        for (auto & arg_ast : index_type->arguments->children)
        {
            auto * equals_fn = arg_ast->as<ASTFunction>();
            if (!equals_fn || equals_fn->name != "equals" || !equals_fn->arguments || equals_fn->arguments->children.size() != 2)
                continue;

            const auto * param_id = equals_fn->arguments->children[0]->as<ASTIdentifier>();
            if (!param_id)
                continue;

            auto & value_ast = equals_fn->arguments->children[1];

            if (param_id->name() == "tokenizer")
            {
                if (value_ast->as<ASTLiteral>() && fuzz_rand() % 5 == 0)
                {
                    /// Swap between no-arg string-form tokenizers.
                    value_ast = make_intrusive<ASTLiteral>(pickRandomly(fuzz_rand, simple_tokenizers));
                }
                else if (auto * tok_fn = value_ast->as<ASTFunction>())
                {
                    if (tok_fn->name == "ngrams" && tok_fn->arguments && !tok_fn->arguments->children.empty() && fuzz_rand() % 5 == 0)
                    {
                        /// ngram_size >= 1
                        tok_fn->arguments->children[0] = make_intrusive<ASTLiteral>(UInt64(fuzz_rand() % 8 + 1));
                    }
                    else if (
                        tok_fn->name == "sparseGrams" && tok_fn->arguments && tok_fn->arguments->children.size() == 3
                        && fuzz_rand() % 5 == 0)
                    {
                        /// min_length in [3, 100], max_length in [min_length, 100],
                        /// min_cutoff_length in [min_length, max_length].
                        auto min_len = UInt64(fuzz_rand() % 8 + 3);
                        auto max_len = std::min(min_len + UInt64(fuzz_rand() % 20 + 1), UInt64(100));
                        auto cutoff = min_len + UInt64(fuzz_rand() % (max_len - min_len + 1));
                        tok_fn->arguments->children[0] = make_intrusive<ASTLiteral>(min_len);
                        tok_fn->arguments->children[1] = make_intrusive<ASTLiteral>(max_len);
                        tok_fn->arguments->children[2] = make_intrusive<ASTLiteral>(cutoff);
                    }
                }
            }
            else if (param_id->name() == "posting_list_codec")
            {
                if (fuzz_rand() % 5 == 0)
                    value_ast = make_intrusive<ASTLiteral>(pickRandomly(fuzz_rand, posting_list_codecs));
            }
            else if (param_id->name() == "dictionary_block_frontcoding_compression")
            {
                if (fuzz_rand() % 5 == 0)
                    value_ast = make_intrusive<ASTLiteral>(UInt64(fuzz_rand() % 2));
            }
            else if (param_id->name() == "dictionary_block_size" || param_id->name() == "posting_list_block_size")
            {
                if (fuzz_rand() % 5 == 0)
                    value_ast = make_intrusive<ASTLiteral>(UInt64(fuzz_rand() % 2048 + 1));
            }
        }
    }

    /// Fuzz index granularity (1/10 probability).
    if (fuzz_rand() % 10 == 0)
        index.granularity = UInt64(1) << (fuzz_rand() % 14); /// 1 to 8192

    /// Randomly swap the index type (1/10 probability).
    if (fuzz_rand() % 10 == 0)
    {
        if (bf_index_types.contains(index_type->name))
        {
            /// Swap between the two BF types, leaving arguments in place.
            index_type->name = (index_type->name == "ngrambf_v1") ? "tokenbf_v1" : "ngrambf_v1";
        }
        else
        {
            /// For text and other simple types, swap to a no-arg type and clear arguments.
            index_type->name = pickRandomly(fuzz_rand, simple_index_types);
            if (index_type->arguments)
                index_type->arguments->children.clear();
        }
    }
}

void QueryFuzzer::fuzzProjectionDeclaration(ASTProjectionDeclaration & projection)
{
    if (!projection.query)
        return;
    auto * select = typeid_cast<ASTProjectionSelectQuery *>(projection.query);
    if (!select || !select->select())
        return;

    /// Occasionally add _part_offset to SELECT before fuzzing,
    /// so the fuzz passes can move/replace it along with other expressions.
    /// _part_offset is valid in projection SELECT and GROUP BY, but NOT in ORDER BY.
    if (fuzz_rand() % 20 == 0)
    {
        select->select()->children.emplace_back(make_intrusive<ASTIdentifier>("_part_offset"));
    }
    fuzzColumnLikeExpressionList(select->select().get());
    /// GROUP BY — ASTProjectionSelectQuery has no ROLLUP/CUBE/GROUPING SETS/TOTALS/HAVING/WHERE
    if (select->groupBy().get())
    {
        if (fuzz_rand() % 50 == 0)
        {
            select->groupBy()->children.clear();
            select->setExpression(ASTProjectionSelectQuery::Expression::GROUP_BY, {});
        }
        else
        {
            if (fuzz_rand() % 20 == 0)
            {
                /// Occasionally add _part_offset to GROUP BY,
                select->groupBy()->children.emplace_back(make_intrusive<ASTIdentifier>("_part_offset"));
            }
            fuzzColumnLikeExpressionList(select->groupBy().get());
        }
    }
    else if (fuzz_rand() % 50 == 0)
    {
        /// Add a GROUP BY when the projection has none.
        select->setExpression(ASTProjectionSelectQuery::Expression::GROUP_BY, getRandomExpressionList(select->select()->children.size()));
    }
    if (select->orderBy().get())
    {
        if (fuzz_rand() % 50 == 0)
        {
            select->orderBy()->children.clear();
            select->setExpression(ASTProjectionSelectQuery::Expression::ORDER_BY, {});
        }
        else
        {
            /// ASTProjectionSelectQuery stores ORDER BY as either a bare ASTOrderByElement
            /// (single key) or an ASTFunction("tuple", arguments=ASTExpressionList)
            /// (multiple keys) — never as a bare ASTExpressionList like ASTSelectQuery.
            auto * as_func = select->orderBy().get()->as<ASTFunction>();
            if (as_func && as_func->name == "tuple" && as_func->arguments)
            {
                fuzzOrderByList(as_func->arguments.get(), 0);
            }
        }
    }
    else if (fuzz_rand() % 50 == 0)
    {
        /// Add an ORDER BY when the projection has none.
        const auto col = getRandomColumnLike();
        if (col)
        {
            auto elem = make_intrusive<ASTOrderByElement>();
            elem->children.emplace_back(col);
            elem->direction = 1;
            elem->nulls_direction = 1;
            elem->nulls_direction_was_explicitly_specified = false;
            elem->with_fill = false;
            select->setExpression(ASTProjectionSelectQuery::Expression::ORDER_BY, std::move(elem));
        }
    }
}


DataTypePtr QueryFuzzer::fuzzDataType(DataTypePtr type)
{
    checkIterationLimit();

    /// Do not replace Array/Tuple/etc. with not Array/Tuple too often.
    const auto * type_array = typeid_cast<const DataTypeArray *>(type.get());
    if (type_array && fuzz_rand() % 4 != 0)
        return std::make_shared<DataTypeArray>(fuzzDataType(type_array->getNestedType()));

    const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get());
    if (type_tuple && fuzz_rand() % 4 != 0)
    {
        DataTypes elements;
        for (const auto & element : type_tuple->getElements())
            elements.push_back(fuzzDataType(element));
        /// Occasionally add a new alternative
        if (elements.size() < 10 && fuzz_rand() % 10 == 0)
            elements.push_back(getRandomType());
        /// Occasionally drop an alternative (keep at least 1)
        if (elements.size() > 1 && fuzz_rand() % 10 == 0)
            elements.erase(elements.begin() + fuzz_rand() % elements.size());
        if (type_tuple->hasExplicitNames())
        {
            auto names = type_tuple->getElementNames();
            /// Pad with synthetic names if a field was added, truncate if one was dropped
            while (names.size() < elements.size())
                names.push_back("f" + std::to_string(names.size()));
            names.resize(elements.size());
            return std::make_shared<DataTypeTuple>(elements, names);
        }
        return std::make_shared<DataTypeTuple>(elements);
    }

    const auto * type_map = typeid_cast<const DataTypeMap *>(type.get());
    if (type_map && fuzz_rand() % 4 != 0)
    {
        auto key_type = fuzzDataType(type_map->getKeyType());
        auto value_type = fuzzDataType(type_map->getValueType());
        if (!DataTypeMap::isValidKeyType(key_type))
            key_type = type_map->getKeyType();

        return std::make_shared<DataTypeMap>(key_type, value_type);
    }

    const auto * type_nullable = typeid_cast<const DataTypeNullable *>(type.get());
    if (type_nullable)
    {
        size_t tmp = fuzz_rand() % 3;
        if (tmp == 0)
            return fuzzDataType(type_nullable->getNestedType());

        if (tmp == 1)
        {
            auto nested_type = fuzzDataType(type_nullable->getNestedType());
            if (nested_type->canBeInsideNullable())
                return std::make_shared<DataTypeNullable>(nested_type);
        }
    }

    const auto * type_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get());
    if (type_low_cardinality)
    {
        size_t tmp = fuzz_rand() % 3;
        if (tmp == 0)
            return fuzzDataType(type_low_cardinality->getDictionaryType());

        if (tmp == 1)
        {
            auto nested_type = fuzzDataType(type_low_cardinality->getDictionaryType());
            if (nested_type->canBeInsideLowCardinality())
                return std::make_shared<DataTypeLowCardinality>(nested_type);
        }
    }

    const auto * type_fixed_string = typeid_cast<const DataTypeFixedString *>(type.get());
    if (type_fixed_string && fuzz_rand() % 4 != 0)
    {
        /// Mutate length by ±2 (relative) or pick a fresh random size
        const size_t n = type_fixed_string->getN();
        const size_t new_n = (fuzz_rand() % 4 == 0)
            ? (fuzz_rand() % MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS + 1)
            : std::clamp<size_t>(n + fuzz_rand() % 5 - 2, 1, MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS);
        return std::make_shared<DataTypeFixedString>(new_n);
    }

    const auto * type_datetime64 = typeid_cast<const DataTypeDateTime64 *>(type.get());
    if (type_datetime64 && fuzz_rand() % 4 != 0)
        return std::make_shared<DataTypeDateTime64>(fuzz_rand() % 10); /// scale in [0, 9]

    const auto * type_dynamic = typeid_cast<const DataTypeDynamic *>(type.get());
    if (type_dynamic && fuzz_rand() % 4 != 0)
        return std::make_shared<DataTypeDynamic>(fuzz_rand() % 255);

    const auto * type_variant = typeid_cast<const DataTypeVariant *>(type.get());
    if (type_variant && fuzz_rand() % 4 != 0)
    {
        DataTypes variants;
        for (const auto & v : type_variant->getVariants())
            variants.push_back(fuzzDataType(v));
        /// Occasionally add a new alternative
        if (variants.size() < 10 && fuzz_rand() % 4 == 0)
            variants.push_back(getRandomType());
        /// Occasionally drop an alternative (keep at least 1)
        if (variants.size() > 1 && fuzz_rand() % 4 == 0)
            variants.erase(variants.begin() + fuzz_rand() % variants.size());
        return std::make_shared<DataTypeVariant>(variants);
    }

    /// NOLINTBEGIN(bugprone-macro-parentheses)
    /// Enum types: add or remove enum values
#define FUZZ_ENUM(INT_TYPE) \
    if (const auto * dt_enum = typeid_cast<const DataTypeEnum<INT_TYPE> *>(type.get()); dt_enum && fuzz_rand() % 4 != 0) \
    { \
        auto values = dt_enum->getValues(); \
        if (values.size() < 50 && fuzz_rand() % 3 == 0) \
        { \
            const auto new_val = static_cast<INT_TYPE>(fuzz_rand()); \
            if (!dt_enum->hasValue(new_val)) \
                values.emplace_back("e" + std::to_string(values.size()), new_val); \
        } \
        if (values.size() > 1 && fuzz_rand() % 3 == 0) \
            values.erase(values.begin() + fuzz_rand() % values.size()); \
        if (!values.empty()) \
            return std::make_shared<DataTypeEnum<INT_TYPE>>(values); \
    }
    FUZZ_ENUM(Int8)
    FUZZ_ENUM(Int16)
#undef FUZZ_ENUM

    /// Decimal types: mutate scale, and occasionally the precision tier too
#define FUZZ_DECIMAL(DT) \
    if (const auto * dt_dec = typeid_cast<const DataTypeDecimal<DT> *>(type.get()); dt_dec && fuzz_rand() % 4 != 0) \
    { \
        const UInt32 max_prec = DataTypeDecimal<DT>::maxPrecision(); \
        const UInt32 new_prec = (fuzz_rand() % 4 == 0) ? UInt32(fuzz_rand() % max_prec + 1) : dt_dec->getPrecision(); \
        return std::make_shared<DataTypeDecimal<DT>>(new_prec, UInt32(fuzz_rand() % (new_prec + 1))); \
    }
    FUZZ_DECIMAL(Decimal32)
    FUZZ_DECIMAL(Decimal64)
    FUZZ_DECIMAL(Decimal128)
    FUZZ_DECIMAL(Decimal256)
#undef FUZZ_DECIMAL
    /// NOLINTEND(bugprone-macro-parentheses)

    size_t tmp = fuzz_rand() % 8;
    if (tmp == 0)
        return std::make_shared<DataTypeArray>(type);

    if (tmp <= 1 && type->canBeInsideNullable())
        return std::make_shared<DataTypeNullable>(type);

    if (tmp <= 2 && type->canBeInsideLowCardinality())
        return std::make_shared<DataTypeLowCardinality>(type);

    if (tmp <= 3)
        return getRandomType();

    return type;
}

DataTypePtr QueryFuzzer::getRandomType()
{
    checkIterationLimit();

    static const std::vector<TypeIndex> random_types
        = {TypeIndex::UInt8,       TypeIndex::UInt16,         TypeIndex::UInt32,   TypeIndex::UInt64,     TypeIndex::UInt128,
           TypeIndex::UInt256,     TypeIndex::Int8,           TypeIndex::Int16,    TypeIndex::Int32,      TypeIndex::Int64,
           TypeIndex::Int128,      TypeIndex::Int256,         TypeIndex::BFloat16, TypeIndex::Float32,    TypeIndex::Float64,
           TypeIndex::Date,        TypeIndex::Date32,         TypeIndex::DateTime, TypeIndex::DateTime64, TypeIndex::String,
           TypeIndex::FixedString, TypeIndex::Enum8,          TypeIndex::Enum16,   TypeIndex::Decimal32,  TypeIndex::Decimal64,
           TypeIndex::Decimal128,  TypeIndex::Decimal256,     TypeIndex::UUID,     TypeIndex::Array,      TypeIndex::Tuple,
           TypeIndex::Nullable,    TypeIndex::LowCardinality, TypeIndex::Map,      TypeIndex::IPv4,       TypeIndex::IPv6,
           TypeIndex::Variant,     TypeIndex::Dynamic,        TypeIndex::Time,     TypeIndex::Time64,     TypeIndex::Object,
           TypeIndex::QBit};

    /// Geo types (Point, Ring, Polygon, MultiPolygon) are custom-named Array aliases with no TypeIndex
    /// of their own, so they are appended after the TypeIndex vector in a unified selection.
    static constexpr const char * geo_type_names[] = {"Point", "Ring", "Polygon", "MultiPolygon"};
    static constexpr size_t n_geo = std::size(geo_type_names);
    const size_t pick = fuzz_rand() % (random_types.size() + n_geo);
    if (pick >= random_types.size())
        return DataTypeFactory::instance().get(geo_type_names[pick - random_types.size()]);

    static constexpr const char * timezones[] = {"UTC", "Europe/Moscow", "America/New_York", "Asia/Tokyo", "Australia/Sydney"};
    const auto type_id = random_types[pick];

/// NOLINTBEGIN(bugprone-macro-parentheses)
#define DISPATCH(DECIMAL) \
    case TypeIndex::DECIMAL: \
        return std::make_shared<DataTypeDecimal<DECIMAL>>( \
            DataTypeDecimal<DECIMAL>::maxPrecision(), (fuzz_rand() % DataTypeDecimal<DECIMAL>::maxPrecision()) + 1);

    switch (type_id)
    {
        case TypeIndex::Tuple: {
            const size_t tuple_size = fuzz_rand() % 6;
            DataTypes elements;
            for (size_t i = 0; i < tuple_size; ++i)
                elements.push_back(getRandomType());
            return std::make_shared<DataTypeTuple>(elements);
        }
        case TypeIndex::Variant: {
            const size_t tuple_size = fuzz_rand() % 6 + 1;
            DataTypes elements;
            for (size_t i = 0; i < tuple_size; ++i)
                elements.push_back(getRandomType());
            return std::make_shared<DataTypeVariant>(elements);
        }
        case TypeIndex::Array:
            return std::make_shared<DataTypeArray>(getRandomType());
        case TypeIndex::Map:
            return std::make_shared<DataTypeMap>(getRandomType(), getRandomType());
        case TypeIndex::LowCardinality: {
            auto inner = getRandomType();
            if (!inner->canBeInsideLowCardinality())
                inner = std::make_shared<DataTypeString>();
            return std::make_shared<DataTypeLowCardinality>(inner);
        }
        case TypeIndex::Nullable: {
            auto inner = getRandomType();
            if (!inner->canBeInsideNullable())
                inner = std::make_shared<DataTypeString>();
            return std::make_shared<DataTypeNullable>(inner);
        }
            DISPATCH(Decimal32)
            DISPATCH(Decimal64)
            DISPATCH(Decimal128)
            DISPATCH(Decimal256)
        case TypeIndex::FixedString:
            return std::make_shared<DataTypeFixedString>(fuzz_rand() % 20 + 1);
        case TypeIndex::Enum8: {
            DataTypeEnum<Int8>::Values values;
            const size_t n = fuzz_rand() % 4 + 1;
            for (size_t i = 0; i < n; ++i)
                values.emplace_back("v" + std::to_string(i), static_cast<Int8>(i));
            return std::make_shared<DataTypeEnum<Int8>>(values);
        }
        case TypeIndex::Enum16: {
            DataTypeEnum<Int16>::Values values;
            const size_t n = fuzz_rand() % 4 + 1;
            for (size_t i = 0; i < n; ++i)
                values.emplace_back("v" + std::to_string(i), static_cast<Int16>(i));
            return std::make_shared<DataTypeEnum<Int16>>(values);
        }
        case TypeIndex::DateTime:
            if (fuzz_rand() % 3 == 0)
                return std::make_shared<DataTypeDateTime>(timezones[fuzz_rand() % std::size(timezones)]);
            return std::make_shared<DataTypeDateTime>();
        case TypeIndex::DateTime64: {
            const UInt32 scale = fuzz_rand() % 10;
            if (fuzz_rand() % 3 == 0)
                return std::make_shared<DataTypeDateTime64>(scale, timezones[fuzz_rand() % std::size(timezones)]);
            return std::make_shared<DataTypeDateTime64>(scale);
        }
        case TypeIndex::Time64:
            return std::make_shared<DataTypeTime64>(fuzz_rand() % 10);
        case TypeIndex::Dynamic:
            return std::make_shared<DataTypeDynamic>(fuzz_rand() % 20);
        case TypeIndex::Object:
            return std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
        case TypeIndex::QBit: {
            static const DataTypePtr qbit_element_types[]
                = {std::make_shared<DataTypeBFloat16>(), std::make_shared<DataTypeFloat32>(), std::make_shared<DataTypeFloat64>()};
            const size_t dimension = fuzz_rand() % 128 + 1;
            return std::make_shared<DataTypeQBit>(qbit_element_types[fuzz_rand() % std::size(qbit_element_types)], dimension);
        }
        default:
            return DataTypeFactory::instance().get(String(magic_enum::enum_name(type_id)));
    }

#undef DISPATCH
    /// NOLINTEND(bugprone-macro-parentheses)
}

void QueryFuzzer::fuzzTableName(ASTTableExpression & table)
{
    if (!table.database_and_table_name || fuzz_rand() % 3 == 0)
        return;

    const auto * identifier = table.database_and_table_name->as<ASTTableIdentifier>();
    if (!identifier)
        return;

    auto table_id = identifier->getTableId();
    if (table_id.empty())
        return;

    auto original_name = getOriginalTableName(table_id.getTableName());
    auto it = original_table_name_to_fuzzed.find(original_name);
    if (it != original_table_name_to_fuzzed.end() && !it->second.empty())
    {
        auto new_table_name = it->second.begin();
        std::advance(new_table_name, fuzz_rand() % it->second.size());
        StorageID new_table_id(table_id.database_name, *new_table_name);
        table.database_and_table_name = make_intrusive<ASTTableIdentifier>(new_table_id);
    }
}

void QueryFuzzer::fuzzExplainQuery(ASTExplainQuery & explain)
{
    explain.setExplainKind(fuzzExplainKind(explain.getKind()));

    bool settings_have_fuzzed = false;
    for (auto & child : explain.children)
    {
        if (auto * settings_ast = typeid_cast<ASTSetQuery *>(child.get()))
        {
            fuzzExplainSettings(*settings_ast, explain.getKind());
            settings_have_fuzzed = true;
        }
        /// Fuzzing other child like Explain Query
        else
        {
            fuzz(child);
        }
    }

    if (!settings_have_fuzzed)
    {
        auto settings_ast = make_intrusive<ASTSetQuery>();
        settings_ast->is_standalone = false;
        fuzzExplainSettings(*settings_ast, explain.getKind());
        explain.setSettings(settings_ast);
    }
}

ASTExplainQuery::ExplainKind QueryFuzzer::fuzzExplainKind(ASTExplainQuery::ExplainKind kind)
{
    if (fuzz_rand() % 20 == 0)
    {
        return kind;
    }
    static const std::vector<ASTExplainQuery::ExplainKind> explain_kinds
        = {ASTExplainQuery::ExplainKind::ParsedAST,
           ASTExplainQuery::ExplainKind::AnalyzedSyntax,
           ASTExplainQuery::ExplainKind::QueryTree,
           ASTExplainQuery::ExplainKind::QueryPlan,
           ASTExplainQuery::ExplainKind::QueryPipeline,
           ASTExplainQuery::ExplainKind::QueryEstimates,
           ASTExplainQuery::ExplainKind::TableOverride,
           ASTExplainQuery::ExplainKind::CurrentTransaction};
    return explain_kinds[fuzz_rand() % explain_kinds.size()];
}

void QueryFuzzer::fuzzExplainSettings(ASTSetQuery & settings_ast, ASTExplainQuery::ExplainKind kind)
{
    auto & changes = settings_ast.changes;

    static const std::unordered_map<ASTExplainQuery::ExplainKind, DB::Strings> settings_by_kind
        = {{ASTExplainQuery::ExplainKind::ParsedAST, {"graph", "optimize"}},
           {ASTExplainQuery::ExplainKind::AnalyzedSyntax, {"oneline", "query_tree_passes"}},
           {ASTExplainQuery::QueryTree, {"run_passes", "dump_passes", "dump_ast", "passes"}},
           {ASTExplainQuery::ExplainKind::QueryPlan, {"header", "description", "actions", "indexes", "optimize", "json", "sorting"}},
           {ASTExplainQuery::ExplainKind::QueryPipeline, {"header", "graph", "compact"}},
           {ASTExplainQuery::ExplainKind::QueryEstimates, {}},
           {ASTExplainQuery::ExplainKind::TableOverride, {}},
           {ASTExplainQuery::ExplainKind::CurrentTransaction, {}}};

    const auto & settings = settings_by_kind.at(kind);
    if (fuzz_rand() % 50 == 0 && !changes.empty())
    {
        changes.erase(changes.begin() + fuzz_rand() % changes.size());
    }

    for (const auto & setting : settings)
    {
        if (fuzz_rand() % 5 == 0)
        {
            changes.emplace_back(setting, true);
        }
    }
}

ASTs QueryFuzzer::getDropQueriesForFuzzedTables(const ASTDropQuery & drop_query)
{
    if (drop_query.kind != ASTDropQuery::Drop)
        return {};

    auto table_name = drop_query.getTable();
    auto it = index_of_fuzzed_table.find(table_name);
    if (it == index_of_fuzzed_table.end())
        return {};

    ASTs queries;
    /// Drop all created tables, not only unique ones.
    for (size_t i = 0; i < it->second; ++i)
    {
        auto fuzzed_name = getFuzzedTableName(table_name, i);
        auto & query = queries.emplace_back(drop_query.clone());
        query->as<ASTDropQuery>()->setTable(fuzzed_name);
        /// Just in case add IF EXISTS to avoid exceptions.
        query->as<ASTDropQuery>()->if_exists = true;
    }

    index_of_fuzzed_table.erase(it);
    original_table_name_to_fuzzed.erase(table_name);

    return queries;
}

void QueryFuzzer::notifyQueryFailed(ASTPtr ast)
{
    if (ast == nullptr)
        return;

    auto remove_fuzzed_table = [this](const auto & table_name)
    {
        auto pos = table_name.find("__fuzz_");
        if (pos != std::string::npos)
        {
            auto original_name = table_name.substr(0, pos);
            auto it = original_table_name_to_fuzzed.find(original_name);
            if (it != original_table_name_to_fuzzed.end())
                it->second.erase(table_name);
        }
    };

    if (const auto * create = ast->as<ASTCreateQuery>())
        remove_fuzzed_table(create->getTable());

    if (const auto * insert = ast->as<ASTInsertQuery>())
        remove_fuzzed_table(insert->getTable());
}

ASTPtr QueryFuzzer::fuzzLiteralUnderExpressionList(ASTPtr child)
{
    checkIterationLimit();

    const auto * l = child->as<ASTLiteral>();
    chassert(l);
    const auto type = l->value.getType();
    if (type == Field::Types::Which::String && fuzz_rand() % 7 == 0)
    {
        const String value = l->value.safeGet<String>();
        child = makeASTFunction(
            "toFixedString", make_intrusive<ASTLiteral>(value), make_intrusive<ASTLiteral>(static_cast<UInt64>(value.size())));
    }
    else if (type == Field::Types::Which::UInt64 && fuzz_rand() % 7 == 0)
    {
        child = makeASTFunction(fuzz_rand() % 2 == 0 ? "toUInt128" : "toUInt256", make_intrusive<ASTLiteral>(l->value.safeGet<UInt64>()));
    }
    else if (type == Field::Types::Which::Int64 && fuzz_rand() % 7 == 0)
    {
        child = makeASTFunction(fuzz_rand() % 2 == 0 ? "toInt128" : "toInt256", make_intrusive<ASTLiteral>(l->value.safeGet<Int64>()));
    }
    else if (type == Field::Types::Which::Float64 && fuzz_rand() % 7 == 0)
    {
        const int decimal = fuzz_rand() % 4;
        if (decimal == 0)
            child = makeASTFunction(
                "toDecimal32",
                make_intrusive<ASTLiteral>(l->value.safeGet<Float64>()),
                make_intrusive<ASTLiteral>(static_cast<UInt64>(fuzz_rand() % 9)));
        else if (decimal == 1)
            child = makeASTFunction(
                "toDecimal64",
                make_intrusive<ASTLiteral>(l->value.safeGet<Float64>()),
                make_intrusive<ASTLiteral>(static_cast<UInt64>(fuzz_rand() % 18)));
        else if (decimal == 2)
            child = makeASTFunction(
                "toDecimal128",
                make_intrusive<ASTLiteral>(l->value.safeGet<Float64>()),
                make_intrusive<ASTLiteral>(static_cast<UInt64>(fuzz_rand() % 38)));
        else
            child = makeASTFunction(
                "toDecimal256",
                make_intrusive<ASTLiteral>(l->value.safeGet<Float64>()),
                make_intrusive<ASTLiteral>(static_cast<UInt64>(fuzz_rand() % 76)));
    }

    if (fuzz_rand() % 7 == 0)
        child = makeASTFunction("toNullable", child);

    if (fuzz_rand() % 7 == 0)
        child = makeASTFunction("toLowCardinality", child);

    if (fuzz_rand() % 7 == 0)
        child = makeASTFunction("materialize", child);

    return child;
}

/// Tries to remove the functions added in fuzzLiteralUnderExpressionList
/// Note that it removes them even if the child is not a literal
ASTPtr QueryFuzzer::reverseLiteralFuzzing(ASTPtr child)
{
    if (auto * function = child.get()->as<ASTFunction>())
    {
        static const std::unordered_set<String> can_be_reverted{
            "materialize",
            "toDecimal32", /// Keeping the first parameter only should be ok (valid query most of the time)
            "toDecimal64",
            "toDecimal128",
            "toDecimal256",
            "toFixedString", /// Same as toDecimal
            "toInt128",
            "toInt256",
            "toLowCardinality",
            "toNullable",
            "toUInt128",
            "toUInt256"};
        if (can_be_reverted.contains(function->name) && function->children.size() == 1)
        {
            if (fuzz_rand() % 7 == 0)
                return function->children[0];
        }
    }

    return nullptr;
}

void QueryFuzzer::fuzzExpressionList(ASTExpressionList & expr_list)
{
    /// Permute list
    if (!expr_list.children.empty() && fuzz_rand() % 20 == 0)
    {
        std::shuffle(expr_list.children.begin(), expr_list.children.end(), fuzz_rand);
    }
    for (auto & child : expr_list.children)
    {
        ASTPtr new_child = nullptr;
        static const constexpr int asterisk_prob = 2000;

        /// ORDER BY lists contain ASTOrderByElement nodes which must not be replaced by
        /// arbitrary expressions — doing so breaks the order_by_all formatting invariant
        /// (ASTSelectQuery::formatImpl assumes children[0] is ASTOrderByElement when
        /// order_by_all == true) and causes a null-pointer crash in format().
        if (!typeid_cast<ASTOrderByElement *>(child.get()))
        {
            if (auto * /*literal*/ _ = typeid_cast<ASTLiteral *>(child.get()))
            {
                /// Return a '*' literal
                if (fuzz_rand() % asterisk_prob == 0)
                    new_child = make_intrusive<ASTAsterisk>();
                else if (fuzz_rand() % 13 == 0)
                    new_child = fuzzLiteralUnderExpressionList(child);
            }
            else if (fuzz_rand() % asterisk_prob == 0 && dynamic_cast<ASTWithAlias *>(child.get()))
            {
                /// Return a '*' literal
                new_child = make_intrusive<ASTAsterisk>();
            }
            else if (fuzz_rand() % 1500 == 0 && current_ast_depth < 80)
            {
                /// Wrap child in a scalar subquery (SELECT child)
                auto sel_list = make_intrusive<ASTExpressionList>();
                sel_list->children.emplace_back(child->clone());
                auto select_query = make_intrusive<ASTSelectQuery>();
                select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(sel_list));
                new_child = make_intrusive<ASTSubquery>(std::move(select_query));
            }
            else if (fuzz_rand() % 1000 == 0)
            {
                /// Wrap child in arithmetic: child op other (or other op child)
                static const Strings arith_ops = {"plus", "minus", "multiply", "divide", "intDiv", "modulo"};
                auto other = getRandomColumnLike();
                if (other)
                {
                    const String & op = arith_ops[fuzz_rand() % arith_ops.size()];
                    if (fuzz_rand() % 2 == 0)
                        new_child = makeASTFunction(op, child, other);
                    else
                        new_child = makeASTFunction(op, other, child);
                }
            }
            else if (fuzz_rand() % 1000 == 0 && current_ast_depth < 80)
            {
                /// Wrap child in if(cond, child, other) or if(cond, other, child)
                ASTPtr cond = generatePredicate();
                auto other = getRandomColumnLike();
                if (cond && other)
                {
                    if (fuzz_rand() % 2 == 0)
                        new_child = makeASTFunction("if", cond, child, other);
                    else
                        new_child = makeASTFunction("if", cond, other, child);
                }
            }
            else if (fuzz_rand() % 800 == 0 && current_ast_depth < 80)
            {
                /// Build multiIf(cond1, e1[, cond2, e2], else): a CASE WHEN expression
                auto multiif_func = make_intrusive<ASTFunction>();
                multiif_func->name = "multiIf";
                multiif_func->arguments = make_intrusive<ASTExpressionList>();
                multiif_func->children.push_back(multiif_func->arguments);
                const int nclauses = (fuzz_rand() % 2) + 1;
                bool ok = true;
                for (int ci = 0; ci < nclauses && ok; ci++)
                {
                    ASTPtr cond = generatePredicate();
                    auto val = getRandomColumnLike();
                    if (cond && val)
                    {
                        multiif_func->arguments->children.emplace_back(cond);
                        multiif_func->arguments->children.emplace_back(val);
                    }
                    else
                        ok = false;
                }
                auto else_val = getRandomColumnLike();
                if (ok && else_val)
                {
                    multiif_func->arguments->children.emplace_back(else_val);
                    new_child = multiif_func;
                }
            }
            else
            {
                new_child = reverseLiteralFuzzing(child);
            }
        }
        if (new_child)
            child = new_child;
        else
            fuzz(child);
    }
}

void QueryFuzzer::fuzz(ASTs & asts)
{
    for (auto & ast : asts)
    {
        fuzz(ast);
    }
}

struct ScopedIncrement
{
    size_t & counter;

    explicit ScopedIncrement(size_t & counter_)
        : counter(counter_)
    {
        ++counter;
    }
    ~ScopedIncrement() { --counter; }
};

void QueryFuzzer::checkIterationLimit()
{
    if (++iteration_count > iteration_limit)
        throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "AST complexity limit exceeded while fuzzing ({})", iteration_count);
}

static const Strings comparison_comparators
    = {"equals", "notEquals", "greater", "greaterOrEquals", "less", "lessOrEquals", "isNotDistinctFrom"};

ASTPtr QueryFuzzer::tryNegateNextPredicate(const ASTPtr & pred, const int prob)
{
    return fuzz_rand() % prob == 0 ? makeASTFunction("not", pred) : pred;
}

ASTPtr QueryFuzzer::setIdentifierAliasOrNot(ASTPtr & exp)
{
    if (auto * walias = dynamic_cast<ASTWithAlias *>(exp.get()))
    {
        const auto & alias = walias->tryGetAlias();

        if (alias.empty() && fuzz_rand() % 50 == 0)
        {
            // Add alias to the expression to be used later on
            const String next_alias = "alias" + std::to_string(alias_counter++);

            walias->setAlias(next_alias);
        }
        else if (!alias.empty())
        {
            ASTIdentifier * id;
            const int next_action = fuzz_rand() % 30;

            if (next_action == 0 && (id = typeid_cast<ASTIdentifier *>(exp.get())) && !id->name_parts.empty())
            {
                /// Move alias to the end of the identifier (most of the time) or somewhere else
                Strings clone_parts = id->name_parts;
                int name_parts_size = static_cast<int>(id->name_parts.size());
                const int index = (fuzz_rand() % 2) == 0 ? (name_parts_size - 1) : (fuzz_rand() % name_parts_size);

                clone_parts[index] = alias;
                return make_intrusive<ASTIdentifier>(std::move(clone_parts));
            }
            else if (next_action == 1)
            {
                /// Replace expression with the alias as an identifier
                return make_intrusive<ASTIdentifier>(Strings{alias});
            }
        }
    }
    else if (fuzz_rand() % 100 != 0)
    {
        exp->setAlias("");
    }
    return exp;
}

static const auto identifier_lambda = [](std::pair<std::string, ASTPtr> & p)
{
    /// No query parameters identifiers at this moment
    const auto * id = typeid_cast<ASTIdentifier *>(p.second.get());
    return id && !id->name_parts.empty() && !id->isParam();
};

ASTPtr QueryFuzzer::generatePredicate()
{
    const int prob = fuzz_rand() % 3;

    if (prob == 0)
    {
        /// Add a predicate, most of the times use a column in one of the sides
        colids.clear();
        if (fuzz_rand() % 10 == 0)
        {
            colids = column_like;
        }
        else
        {
            std::copy_if(column_like.begin(), column_like.end(), std::back_inserter(colids), identifier_lambda);
        }
        if (!colids.empty())
        {
            ASTPtr predicate = nullptr;
            const int nconditions = (fuzz_rand() % 10) < 8 ? 1 : ((fuzz_rand() % 5) + 1);

            for (int i = 0; i < nconditions; i++)
            {
                ASTPtr next_condition = nullptr;
                const int nprob = fuzz_rand() % 12;

                /// Pick a random identifier
                auto rand_col1 = colids.begin();
                std::advance(rand_col1, fuzz_rand() % colids.size());
                ASTPtr expression_1 = rand_col1->second->clone();

                expression_1 = setIdentifierAliasOrNot(expression_1);
                if (nprob == 0)
                {
                    next_condition = makeASTFunction(fuzz_rand() % 2 == 0 ? "isNull" : "isNotNull", expression_1);
                }
                else if (nprob == 1 && !table_like.empty() && current_ast_depth < 80)
                {
                    /// col IN/NOT IN/globalIn/globalNotIn (subquery), or EXISTS (subquery)
                    static const Strings subquery_variants = {"in", "notIn", "globalIn", "globalNotIn", "exists"};
                    for (size_t att = 0; att < 10; ++att)
                    {
                        const auto & entry = table_like[fuzz_rand() % table_like.size()];
                        if (typeid_cast<ASTSubquery *>(entry.second.get()))
                        {
                            const String & variant = subquery_variants[fuzz_rand() % subquery_variants.size()];
                            if (variant == "exists")
                                next_condition = makeASTFunction(variant, entry.second->clone());
                            else
                                next_condition = makeASTFunction(variant, expression_1, entry.second->clone());
                            break;
                        }
                    }
                }
                else if (nprob == 2)
                {
                    /// col IN (expr1, expr2, ...) or col NOT IN (...) with a literal tuple
                    static const Strings in_tuple_variants = {"in", "notIn", "globalIn", "globalNotIn"};
                    auto tuple_func = make_intrusive<ASTFunction>();
                    tuple_func->name = "tuple";
                    tuple_func->arguments = make_intrusive<ASTExpressionList>();
                    tuple_func->children.push_back(tuple_func->arguments);
                    const size_t n_items = (fuzz_rand() % 4) + 1;
                    for (size_t j = 0; j < n_items; j++)
                    {
                        auto rand_col = column_like.begin();
                        std::advance(rand_col, fuzz_rand() % column_like.size());
                        tuple_func->arguments->children.push_back(rand_col->second->clone());
                    }
                    next_condition = makeASTFunction(in_tuple_variants[fuzz_rand() % in_tuple_variants.size()], expression_1, tuple_func);
                }
                /// Fall back to a column comparison if no subquery was available (case 1) or for nprob >= 3
                if (!next_condition)
                {
                    /// Pick any other column reference
                    auto rand_col2 = column_like.begin();
                    std::advance(rand_col2, fuzz_rand() % column_like.size());
                    ASTPtr expression_2 = rand_col2->second->clone();

                    expression_2 = setIdentifierAliasOrNot(expression_2);
                    if (fuzz_rand() % 3 == 0)
                    {
                        /// Swap sides
                        auto expression_3 = expression_1;
                        expression_1 = expression_2;
                        expression_2 = expression_3;
                    }
                    /// Run mostly equality conditions
                    /// No isNotDistinctFrom outside join conditions
                    next_condition = makeASTFunction(
                        comparison_comparators[(fuzz_rand() % 10 == 0) ? (fuzz_rand() % (comparison_comparators.size() - 1)) : 0],
                        expression_1,
                        expression_2);
                }
                next_condition = tryNegateNextPredicate(next_condition, 30);
                /// Sometimes use multiple conditions
                predicate = predicate ? makeASTFunction((fuzz_rand() % 10) < 3 ? "or" : "and", predicate, next_condition) : next_condition;
                predicate = tryNegateNextPredicate(predicate, 50);
            }
            return predicate;
        }
    }
    else if (prob == 1)
    {
        return getRandomColumnLike();
    }
    return nullptr;
}

/// Recursive function to extract all predicates from a AND/OR/XOR tree
void QueryFuzzer::extractPredicates(const ASTPtr & node, ASTs & predicates, const std::string & op, const int negProb)
{
    if (const auto * func = node->as<ASTFunction>())
    {
        if (func->name == op)
        {
            /// Recursively extract predicates from children
            for (const auto & entry : func->arguments->children)
            {
                extractPredicates(entry, predicates, op, negProb);
            }
            return;
        }
        if (func->name == "and" || func->name == "or" || func->name == "xor")
        {
            /// Hit another AND/OR/XOR tree, permute it recursively
            predicates.emplace_back(permutePredicateClause(node, negProb));
            return;
        }
    }
    /// It's not a AND/OR/XOR function, add it to the list to be shuffled
    predicates.emplace_back(node);
}

/// Permute a list of conditions under a AND/OR/XOR tree
ASTPtr QueryFuzzer::permutePredicateClause(const ASTPtr & predicate, const int negProb)
{
    if (const auto * func = predicate->as<ASTFunction>())
    {
        if (func->name == "and" || func->name == "or" || func->name == "xor")
        {
            ASTs predicates;

            /// Extract all predicates under the current logical operator
            extractPredicates(predicate, predicates, func->name, negProb);
            chassert(!predicates.empty());
            /// Shuffle them
            std::shuffle(predicates.begin(), predicates.end(), fuzz_rand);
            for (auto & entry : predicates)
            {
                /// Try to negate an entry
                entry = tryNegateNextPredicate(entry, negProb);
            }
            return makeASTFunction(func->name, predicates);
        }
    }
    return tryNegateNextPredicate(predicate, negProb);
}

void QueryFuzzer::fuzzMandatoryPredicate(ASTPtr & predicate, ASTs & children)
{
    if (!predicate)
        return;

    ASTPtr new_pred;
    if (fuzz_rand() % 50 == 0)
        new_pred = generatePredicate();
    else if (fuzz_rand() % 50 == 0)
    {
        auto permuted = permutePredicateClause(predicate, 10);
        if (permuted != predicate)
            new_pred = permuted;
    }

    if (new_pred)
    {
        for (auto & child : children)
            if (child == predicate)
                child = new_pred;
        predicate = new_pred;
    }
}

void QueryFuzzer::addOrReplacePredicate(ASTSelectQuery * sel, const ASTSelectQuery::Expression expr)
{
    if (fuzz_rand() % 50 == 0)
    {
        /// Remove the predicate
        sel->setExpression(expr, {});
        return;
    }
    ASTPtr new_pred = generatePredicate();

    if (new_pred)
    {
        ASTPtr res = new_pred;
        ASTPtr old_pred = sel->getExpression(expr, false);

        if (old_pred && fuzz_rand() % 3 == 0)
        {
            /// Add to existing predicate
            if (fuzz_rand() % 3 == 0)
            {
                /// Swap sides
                auto exp3 = old_pred;
                old_pred = new_pred;
                new_pred = exp3;
            }
            res = makeASTFunction((fuzz_rand() % 10) < 3 ? "or" : "and", new_pred, old_pred);
        }
        sel->setExpression(expr, std::move(res));
    }

    ASTPtr pred_expr = sel->getExpression(expr, false);
    if (pred_expr && fuzz_rand() % 10 == 0)
    {
        /// Permute predicate list
        ASTPtr new_pred_expr = permutePredicateClause(pred_expr, 10);

        if (new_pred_expr != pred_expr)
        {
            sel->setExpression(expr, std::move(new_pred_expr));
        }
    }
}

void QueryFuzzer::fuzzJoinType(ASTTableJoin * table_join)
{
    if (table_join->kind < JoinKind::Cross)
    {
        static const std::vector<JoinLocality> locality_values = {JoinLocality::Unspecified, JoinLocality::Local, JoinLocality::Global};
        static const std::vector<JoinStrictness> all_strictness_values
            = {JoinStrictness::Unspecified,
               JoinStrictness::Any,
               JoinStrictness::All,
               JoinStrictness::Asof,
               JoinStrictness::Anti,
               JoinStrictness::Semi};
        static const std::vector<JoinStrictness> right_strictness_values
            = {JoinStrictness::Unspecified, JoinStrictness::RightAny, JoinStrictness::All, JoinStrictness::Semi, JoinStrictness::Anti};
        static const std::vector<JoinKind> kind_values
            = {JoinKind::Inner, JoinKind::Left, JoinKind::Right, JoinKind::Full /*,JoinKind::Cross,JoinKind::Comma,JoinKind::Paste*/};

        table_join->locality = locality_values[fuzz_rand() % locality_values.size()];
        table_join->kind = kind_values[fuzz_rand() % kind_values.size()];
        if (fuzz_rand() % 2 == 0)
        {
            table_join->strictness = JoinStrictness::Unspecified;
        }
        else
        {
            switch (table_join->kind)
            {
                case JoinKind::Inner:
                    /// Semi inner join not possible
                    table_join->strictness = all_strictness_values[fuzz_rand() % (all_strictness_values.size() - 2)];
                    break;
                case JoinKind::Left:
                    table_join->strictness = all_strictness_values[fuzz_rand() % all_strictness_values.size()];
                    break;
                case JoinKind::Right:
                    table_join->strictness = right_strictness_values[fuzz_rand() % right_strictness_values.size()];
                    break;
                case JoinKind::Full:
                    table_join->strictness = fuzz_rand() % 2 == 0 ? JoinStrictness::Unspecified : JoinStrictness::All;
                    break;
                default:
                    break;
            }
        }
    }
}

static String getOldALias(const ASTPtr & input)
{
    if (const auto * texp = typeid_cast<const ASTTableExpression *>(input.get()))
    {
        const auto & child = texp->database_and_table_name ? texp->database_and_table_name
                                                           : (texp->table_function ? texp->table_function : texp->subquery);
        const auto * walias = dynamic_cast<const ASTWithAlias *>(child.get());
        return walias ? walias->tryGetAlias() : "";
    }
    else if (const auto * sub = dynamic_cast<const ASTWithAlias *>(input.get()))
    {
        return sub->tryGetAlias();
    }
    else
    {
        chassert(false);
        return "";
    }
}

ASTPtr QueryFuzzer::addJoinClause()
{
    /// Add a join clause to the AST
    colids.clear();
    std::copy_if(column_like.begin(), column_like.end(), std::back_inserter(colids), identifier_lambda);
    if (!table_like.empty() && !colids.empty())
    {
        ASTPtr table_exp;
        ASTPtr join_condition;
        auto table_join = make_intrusive<ASTTableJoin>();

        fuzzJoinType(table_join.get());
        /// Add a table to the query
        auto rand_table = table_like.begin();
        std::advance(rand_table, fuzz_rand() % table_like.size());

        const ASTPtr & input_table = rand_table->second;
        const String next_alias = "alias" + std::to_string(alias_counter++);
        const String old_alias = getOldALias(input_table);

        if (old_alias.empty() || fuzz_rand() % 2 == 0)
        {
            /// It has no alias or overwrite old one
            if (typeid_cast<ASTTableExpression *>(input_table.get()))
            {
                table_exp = input_table->clone();
                auto * otexp = typeid_cast<ASTTableExpression *>(table_exp.get());
                auto & otable_exp_child = otexp->database_and_table_name
                    ? otexp->database_and_table_name
                    : (otexp->table_function ? otexp->table_function : otexp->subquery);
                auto * otable_exp_alias = dynamic_cast<ASTWithAlias *>(otable_exp_child.get());
                if (otable_exp_alias)
                {
                    otable_exp_alias->setAlias(next_alias);
                }
            }
            else if (dynamic_cast<ASTWithAlias *>(input_table.get()))
            {
                ASTPtr child = input_table->clone();
                table_exp = make_intrusive<ASTTableExpression>();
                auto * ntexp = typeid_cast<ASTTableExpression *>(table_exp.get());

                child->setAlias(next_alias);
                ntexp->children.push_back(child);
                if (typeid_cast<ASTSubquery *>(input_table.get()))
                {
                    ntexp->subquery = ntexp->children.back();
                }
                else if (typeid_cast<ASTFunction *>(input_table.get()))
                {
                    ntexp->table_function = ntexp->children.back();
                }
                else
                {
                    ntexp->database_and_table_name = ntexp->children.back();
                }
            }
            else
            {
                chassert(false);
            }
        }
        else
        {
            /// It already has an alias, so make a reference to it
            table_exp = make_intrusive<ASTTableExpression>();
            auto * ntexp = typeid_cast<ASTTableExpression *>(table_exp.get());
            auto new_identifier = make_intrusive<ASTTableIdentifier>(old_alias);
            new_identifier->setAlias(next_alias);
            ntexp->children.push_back(new_identifier);
            ntexp->database_and_table_name = ntexp->children.back();
        }

        const int nconditions = (fuzz_rand() % 10) < 8 ? 1 : ((fuzz_rand() % 5) + 1);
        for (int i = 0; i < nconditions; i++)
        {
            /// Pick a random column
            auto rand_col1 = colids.begin();
            std::advance(rand_col1, fuzz_rand() % colids.size());
            const auto * id1 = typeid_cast<ASTIdentifier *>(rand_col1->second.get());

            /// Use another random column
            /// Most of the times, search from the identifier list
            const auto & to_search = fuzz_rand() % 10 == 0 ? column_like : colids;
            auto rand_col2 = to_search.begin();
            std::advance(rand_col2, fuzz_rand() % to_search.size());

            const String id1_alias = id1->tryGetAlias();
            const String & nidentifier = (id1_alias.empty() || (fuzz_rand() % 2 == 0)) ? id1->shortName() : id1_alias;
            ASTPtr expression_1 = make_intrusive<ASTIdentifier>(Strings{next_alias, nidentifier});
            ASTPtr expression_2 = rand_col2->second->clone();

            expression_2 = setIdentifierAliasOrNot(expression_2);
            if (fuzz_rand() % 3 == 0)
            {
                /// Swap sides
                auto expression_e = expression_1;
                expression_1 = expression_2;
                expression_2 = expression_e;
            }
            /// Run mostly equi-joins
            ASTPtr next_condition = makeASTFunction(
                comparison_comparators[(fuzz_rand() % 10 == 0) ? (fuzz_rand() % comparison_comparators.size()) : 0],
                expression_1,
                expression_2);
            next_condition = tryNegateNextPredicate(next_condition, 30);

            /// Sometimes use multiple conditions
            join_condition
                = join_condition ? makeASTFunction((fuzz_rand() % 10) == 0 ? "or" : "and", join_condition, next_condition) : next_condition;
            join_condition = tryNegateNextPredicate(join_condition, 50);
        }
        chassert(join_condition);
        table_join->children.push_back(join_condition);
        table_join->on_expression = table_join->children.back();

        auto table = make_intrusive<ASTTablesInSelectQueryElement>();
        table->table_join = table_join;
        table->table_expression = table_exp;
        return table;
    }
    return nullptr;
}

ASTPtr QueryFuzzer::addArrayJoinClause()
{
    /// Add an array join clause to the AST
    const ASTPtr arr_join_list = getRandomExpressionList(0);
    if (arr_join_list)
    {
        auto array_join = make_intrusive<ASTArrayJoin>();
        array_join->kind = fuzz_rand() % 2 == 0 ? ASTArrayJoin::Kind::Left : ASTArrayJoin::Kind::Inner;
        array_join->children.push_back(arr_join_list);
        array_join->expression_list = array_join->children.back();

        auto table_join = make_intrusive<ASTTablesInSelectQueryElement>();
        table_join->children.push_back(std::move(array_join));
        table_join->array_join = table_join->children.back();
        return table_join;
    }
    return nullptr;
}

static const std::map<size_t, Strings> swapAggrs
    = {{1, {"any",          "anyHeavy",
            "anyLast",      "anyRespectNulls",
            "avg",          "count",
            "deltaSum",     "entropy",
            "first_value",  "groupArray",
            "groupBitAnd",  "groupBitOr",
            "groupBitXor",  "groupUniqArray",
            "kurtPop",      "kurtSamp",
            "last_value",   "max",
            "median",       "min",
            "rankCorr",     "singleValueOrNull",
            "skewPop",      "skewSamp",
            "stddevPop",    "stddevPopStable",
            "stddevSamp",   "stddevSampStable",
            "sum",          "sumCount",
            "sumKahan",     "sumWithOverflow",
            "topK",         "uniq",
            "uniqCombined", "uniqCombined64",
            "uniqExact",    "uniqHLL12",
            "uniqTheta",    "varPop",
            "varPopStable", "varSamp",
            "varSampStable"}},
       {2,
        {"argMax",
         "argMin",
         "avgWeighted",
         "boundingRatio",
         "contingency",
         "corr",
         "corrStable",
         "covarPop",
         "covarPopStable",
         "covarSamp",
         "covarSampStable",
         "cramersV",
         "cramersVBiasCorrected",
         "deltaSumTimestamp",
         "kolmogorovSmirnovTest",
         "mannWhitneyUTest",
         "maxIntersections",
         "maxIntersectionsPosition",
         "quantileWeighted",
         "studentTTest",
         "theilsU",
         "topKWeighted",
         "uniq",
         "welchTTest"}}};

static const std::vector<std::unordered_set<String>> & swapFuncs
    = { /// String pattern matching operators
        {"ilike", "like", "match", "notILike", "notLike"},
        /// Set membership operators
        {"globalIn", "globalNotIn", "in", "notIn"},
        /// Null predicate and conversion functions
        {"assumeNotNull", "isNotNull", "isNull", "isNullable", "isZeroOrNull", "toNullable"},
        /// Value selection / clamping / null-coalescing
        {"clamp", "coalesce", "firstNonDefault", "greatest", "ifNull", "least"},
        /// Comparison operators
        {"equals", "notEquals", "greater", "greaterOrEquals", "less", "lessOrEquals", "isNotDistinctFrom"},
        /// Arithmetic and string operators
        {"concat", "divide", "intDiv", "intDivOrZero", "minus", "modulo", "moduloOrZero", "multiply", "plus"},
        /// Date/time component extractors and truncators (date/datetime → numeric or date)
        {"toDayOfMonth",
         "toDayOfWeek",
         "toDayOfYear",
         "toDaysSinceYearZero",
         "toHour",
         "toISOWeek",
         "toISOYear",
         "toLastDayOfMonth",
         "toLastDayOfWeek",
         "toMillisecond",
         "toMinute",
         "toMonday",
         "toMonth",
         "toQuarter",
         "toRelativeDayNum",
         "toRelativeHourNum",
         "toRelativeMinuteNum",
         "toRelativeMonthNum",
         "toRelativeQuarterNum",
         "toRelativeSecondNum",
         "toRelativeWeekNum",
         "toRelativeYearNum",
         "toSecond",
         "toStartOfDay",
         "toStartOfFifteenMinutes",
         "toStartOfFiveMinutes",
         "toStartOfHour",
         "toStartOfISOYear",
         "toStartOfMicrosecond",
         "toStartOfMillisecond",
         "toStartOfMinute",
         "toStartOfMonth",
         "toStartOfNanosecond",
         "toStartOfQuarter",
         "toStartOfSecond",
         "toStartOfTenMinutes",
         "toStartOfWeek",
         "toStartOfYear",
         "toTime",
         "toUnixTimestamp",
         "toWeek",
         "toYear",
         "toYearWeek",
         "toYYYYMM",
         "toYYYYMMDD",
         "toYYYYMMDDhhmmss"},
        /// Interval constructors (number → Interval)
        {"toIntervalDay",
         "toIntervalHour",
         "toIntervalMicrosecond",
         "toIntervalMillisecond",
         "toIntervalMinute",
         "toIntervalMonth",
         "toIntervalNanosecond",
         "toIntervalQuarter",
         "toIntervalSecond",
         "toIntervalWeek",
         "toIntervalYear"},
        /// Unix timestamp at sub-second precision (datetime64 → Int64)
        {"toUnixTimestamp64Micro", "toUnixTimestamp64Milli", "toUnixTimestamp64Nano", "toUnixTimestamp64Second"},
        /// Date arithmetic: add/subtract intervals (date/datetime, number → datetime)
        {"addDays",         "addHours",       "addInterval",         "addMicroseconds",  "addMilliseconds",      "addMinutes",
         "addMonths",       "addNanoseconds", "addQuarters",         "addSeconds",       "addTupleOfIntervals",  "addWeeks",
         "addYears",        "subtractDays",   "subtractHours",       "subtractInterval", "subtractMicroseconds", "subtractMilliseconds",
         "subtractMinutes", "subtractMonths", "subtractNanoseconds", "subtractQuarters", "subtractSeconds",      "subtractTupleOfIntervals",
         "subtractWeeks",   "subtractYears"},
        /// Decimal type casts (value, precision, scale → Decimal)
        {"toDecimal32", "toDecimal64", "toDecimal128", "toDecimal256"},
        /// Integer type casts
        {"toInt8",
         "toInt16",
         "toInt32",
         "toInt64",
         "toInt128",
         "toInt256",
         "toUInt8",
         "toUInt16",
         "toUInt32",
         "toUInt64",
         "toUInt128",
         "toUInt256"},
        /// Floating-point type casts
        {"toBFloat16", "toFloat32", "toFloat64"},
        /// Date/datetime type casts
        {"toDate", "toDate32", "toDateTime", "toDateTime32", "toDateTime64", "toTime", "toTime64"},
        /// Rounding functions (number → number)
        {"ceil", "floor", "round", "roundBankers", "roundDown", "trunc"},
        /// Bitwise binary operators
        {"bitAnd", "bitOr", "bitXor"},
        /// Bit shift operators
        {"bitShiftLeft", "bitShiftRight"},
        /// String case, length, and validity functions (string → string or UInt64)
        {"upper", "lower", "lowerUTF8", "upperUTF8", "reverse", "reverseUTF8", "length", "lengthUTF8", "isValidASCII", "isValidUTF8"},
        /// String left/right extraction and padding
        {"right", "rightPad", "rightPadUTF8", "rightUTF8", "left", "leftPad", "leftPadUTF8", "leftUTF8"},
        /// Whitespace trimming
        {"trim", "trimBoth", "trimLeft", "trimRight"},
        /// Emptiness predicates (string/array → UInt8)
        {"empty", "notEmpty"},
        /// Array/string containment checks
        {"has",
         "hasAll",
         "hasAny",
         "hasToken",
         "hasTokenCaseInsensitive",
         "hasTokenCaseInsensitiveOrNull",
         "hasTokenOrNull",
         "hasAnyTokens",
         "hasAllTokens"},
        /// Map containment checks
        {"mapContains", "mapContainsKey", "mapContainsKeyLike", "mapContainsValue", "mapContainsValueLike"},
        /// Prefix/suffix predicates (string, string → UInt8)
        {"startsWith",
         "startsWithUTF8",
         "startsWithCaseInsensitive",
         "startsWithCaseInsensitiveUTF8",
         "endsWith",
         "endsWithUTF8",
         "endsWithCaseInsensitive",
         "endsWithCaseInsensitiveUTF8"},
        /// Vector distance metrics
        {"cosineDistance", "L1Distance", "L2Distance", "L2SquaredDistance", "LinfDistance"},
        /// Array scalar reductions (array → scalar)
        {"arrayMin", "arrayMax", "arraySum", "arrayProduct", "arrayAvg", "arrayUniq"},
        /// Array transform functions (array → array)
        {"arraySort",
         "arrayReverseSort",
         "arrayReverse",
         "arrayShuffle",
         "arrayDistinct",
         "arrayCompact",
         "arrayFlatten",
         "arrayPopFront",
         "arrayPopBack",
         "arrayEnumerate",
         "arrayEnumerateUniq"},
        /// URL hierarchy generators (url → Array(String))
        {"URLHierarchy", "URLPathHierarchy"},
        /// Trig functions, logarithms, exponentials and roots (number → Float64)
        {"sin",   "sinh",    "cos",     "cosh",  "tan",   "tanh",   "asin",     "asinh",   "acos",   "acosh",  "atan",
         "atanh", "log",     "log2",    "log1p", "log10", "lgamma", "intExp10", "intExp2", "ln",     "exp",    "exp2",
         "exp10", "degrees", "radians", "sqrt",  "cbrt",  "erf",    "erfc",     "power",   "tgamma", "sigmoid"},
        /// Non-cryptographic hash functions
        {"cityHash64",
         "CRC32",
         "CRC32IEEE",
         "CRC64ECMA",
         "farmHash64",
         "halfMD5",
         "intHash32",
         "intHash64",
         "murmurHash2_32",
         "murmurHash2_64",
         "murmurHash3_64",
         "sipHash64",
         "xxHash32",
         "xxHash64"},
        /// Cryptographic hashes (string → FixedString)
        {"MD5", "SHA1", "SHA224", "SHA256", "SHA384", "SHA512", "SHA512_256"},
        /// String position search (haystack, needle → UInt64)
        {"position", "positionCaseInsensitive", "positionUTF8", "positionCaseInsensitiveUTF8"},
        /// URL component extractors (url → String)
        {"domain",
         "domainWithoutWWW",
         "topLevelDomain",
         "protocol",
         "path",
         "queryString",
         "fragment",
         "firstSignificantSubdomain",
         "cutToFirstSignificantSubdomain"},
        /// Float classification
        {"isNaN", "isInfinite", "isFinite"},
        /// Map accessors (map → array)
        {"mapKeys", "mapValues"},
        /// Numeric map arithmetic (map, map → map, element-wise)
        {"mapAdd", "mapSubtract"},
        /// Map sorting (map → map)
        {"mapSort", "mapReverseSort"},
        /// Higher-order map transforms (lambda, map → map)
        {"mapFilter", "mapApply"},
        /// Higher-order map predicates (lambda, map → UInt8)
        {"mapExists", "mapAll"},
        /// Binary encoding (bytes → encoded String)
        {"hex", "bin", "base64Encode", "base64URLEncode"},
        /// Binary decoding (encoded String → bytes)
        {"unhex", "unbin", "base64Decode", "base64URLDecode", "tryBase64Decode", "tryBase64URLDecode"},
        /// Sign/magnitude
        {"abs", "sign"},
        /// JSONExtract* family (json, path → typed value)
        {"JSONExtractBool", "JSONExtractFloat", "JSONExtractInt", "JSONExtractRaw", "JSONExtractString", "JSONExtractUInt"},
        /// SQL/JSON standard functions
        {"JSON_EXISTS", "JSON_VALUE", "JSON_QUERY"},
        /// simpleJSON* family (json, path → typed value, no schema)
        {"simpleJSONHas",
         "simpleJSONExtractBool",
         "simpleJSONExtractFloat",
         "simpleJSONExtractInt",
         "simpleJSONExtractRaw",
         "simpleJSONExtractString",
         "simpleJSONExtractUInt"},
        /// String substring extraction (str, offset[, length] → String)
        {"substring", "substringUTF8", "mid", "substr"},
        /// String replacement (str, pattern, replacement → String)
        {"replaceAll", "replaceOne", "replaceRegexpAll", "replaceRegexpOne"},
        /// String splitting (str, sep → Array(String))
        {"splitByChar", "splitByString", "splitByRegexp", "splitByWhitespace", "splitByNonAlpha"},
        /// Substring occurrence count (haystack, needle → UInt64)
        {"countSubstrings", "countSubstringsCaseInsensitive"},
        /// UTF-8 normalization (String → String)
        {"normalizeUTF8NFC", "normalizeUTF8NFD", "normalizeUTF8NFKC", "normalizeUTF8NFKD"},
        /// Bitwise unary (integer → integer)
        {"bitNot", "bitCount"},
        /// Bitmap binary operations (Bitmap, Bitmap → Bitmap)
        {"bitmapAnd", "bitmapOr", "bitmapXor", "bitmapAndnot"},
        /// IP address type casts (String → IPv4/IPv6)
        {"toIPv4", "toIPv4OrNull", "toIPv4OrZero"},
        {"toIPv6", "toIPv6OrNull", "toIPv6OrZero"}};

void QueryFuzzer::fuzz(ASTPtr & ast)
{
    if (!ast)
        return;

    checkIterationLimit();

    // Check for exceeding max depth.
    ScopedIncrement depth_increment(current_ast_depth);
    if (current_ast_depth > 500)
    {
        // The AST is too deep (see the comment for current_ast_depth). Throw
        // an exception to fail fast and not use this query as an etalon, or we'll
        // end up in a very slow and useless loop. It also makes sense to set it
        // lower than the default max parse depth on the server (1000), so that
        // we don't get the useless error about parse depth from the server either.
        throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "AST depth exceeded while fuzzing ({})", current_ast_depth);
    }

    // Check for loops.
    auto [_, inserted] = debug_visited_nodes.insert(ast.get());
    if (!inserted)
    {
        fmt::print(
            stderr,
            "The AST node '{}' was already visited before."
            " Depth {}, {} visited nodes, current top AST:\n{}\n",
            static_cast<void *>(ast.get()),
            current_ast_depth,
            debug_visited_nodes.size(),
            (*debug_top_ast)->dumpTree());
        std::abort();
    }

    // The fuzzing.
    if (auto * with_union = typeid_cast<ASTSelectWithUnionQuery *>(ast.get()))
    {
        if (fuzz_rand() % 20 == 0)
        {
            with_union->union_mode
                = static_cast<SelectUnionMode>(fuzz_rand() % (static_cast<int>(SelectUnionMode::INTERSECT_DISTINCT) + 1));
        }
        auto & union_members = with_union->list_of_selects->children;
        if (union_members.size() > 1 && fuzz_rand() % 100 == 0)
        {
            /// Drop a random member from the UNION; rebuild modes to keep sizes in sync.
            /// After normalization, list_of_modes may be stale (wrong size), so we
            /// rebuild it entirely from union_mode rather than trying to erase one entry.
            union_members.erase(union_members.begin() + fuzz_rand() % union_members.size());
            with_union->list_of_modes.assign(union_members.empty() ? 0 : union_members.size() - 1, with_union->union_mode);
            with_union->is_normalized = false;
        }
        else if (!union_members.empty() && fuzz_rand() % 100 == 0)
        {
            /// Duplicate a random member; rebuild modes to keep sizes in sync.
            union_members.push_back(union_members[fuzz_rand() % union_members.size()]->clone());
            with_union->list_of_modes.assign(union_members.size() - 1, with_union->union_mode);
        }

        fuzz(with_union->list_of_selects);
        /// Fuzzing SELECT query to EXPLAIN query randomly.
        /// And we only fuzzing the root query into an EXPLAIN query, not fuzzing subquery
        if (fuzz_rand() % 20 == 0 && current_ast_depth <= 1)
        {
            auto explain = make_intrusive<ASTExplainQuery>(fuzzExplainKind());

            auto settings_ast = make_intrusive<ASTSetQuery>();
            settings_ast->is_standalone = false;
            fuzzExplainSettings(*settings_ast, explain->getKind());
            explain->setSettings(settings_ast);

            explain->setExplainedQuery(ast);
            ast = explain;
        }
    }
    else if (auto * with_intersect_except = typeid_cast<ASTSelectIntersectExceptQuery *>(ast.get()))
    {
        if (fuzz_rand() % 20 == 0)
        {
            with_intersect_except->final_operator = static_cast<ASTSelectIntersectExceptQuery::Operator>(
                fuzz_rand() % (static_cast<int>(ASTSelectIntersectExceptQuery::Operator::INTERSECT_DISTINCT) + 1));
        }
        auto selects = with_intersect_except->getListOfSelects();
        fuzz(selects);
    }
    else if (auto * tables = typeid_cast<ASTTablesInSelectQuery *>(ast.get()))
    {
        fuzz(tables->children);
    }
    else if (auto * tables_element = typeid_cast<ASTTablesInSelectQueryElement *>(ast.get()))
    {
        fuzz(tables_element->table_join);
        fuzz(tables_element->table_expression);
        fuzz(tables_element->array_join);
    }
    else if (auto * table_expr = typeid_cast<ASTTableExpression *>(ast.get()))
    {
        if (fuzz_rand() % 30 == 0)
        {
            table_expr->final = !table_expr->final;
        }
        fuzzTableName(*table_expr);

        /// Fuzz SAMPLE clause
        if (table_expr->sample_size)
        {
            /// Occasionally remove the SAMPLE clause entirely
            if (fuzz_rand() % 50 == 0)
            {
                auto & ch = table_expr->children;
                ch.erase(std::remove(ch.begin(), ch.end(), table_expr->sample_size), ch.end());
                if (table_expr->sample_offset)
                    ch.erase(std::remove(ch.begin(), ch.end(), table_expr->sample_offset), ch.end());
                table_expr->sample_size = {};
                table_expr->sample_offset = {};
            }
        }
        else if (fuzz_rand() % 50 == 0)
        {
            /// Add a random SAMPLE clause
            static const std::vector<std::pair<UInt64, UInt64>> sample_ratios = {{1, 2}, {1, 10}, {1, 100}, {1, 1000}, {1, 1}, {2, 10}};
            const auto & [num, den] = sample_ratios[fuzz_rand() % sample_ratios.size()];
            ASTSampleRatio::Rational r;
            r.numerator = num;
            r.denominator = den;
            auto sample_node = make_intrusive<ASTSampleRatio>(r);
            table_expr->sample_size = sample_node;
            table_expr->children.push_back(sample_node);
        }

        fuzz(table_expr->children);
    }
    else if (auto * expr_list = typeid_cast<ASTExpressionList *>(ast.get()))
    {
        fuzzExpressionList(*expr_list);
    }
    else if (auto * order_by_element = typeid_cast<ASTOrderByElement *>(ast.get()))
    {
        fuzzOrderByElement(order_by_element);
        fuzz(order_by_element->children);
    }
    else if (auto * fn = typeid_cast<ASTFunction *>(ast.get()))
    {
        static const std::unordered_set<String> cast_functions = {"_CAST", "CAST", "accurateCast", "accurateCastOrNull"};

        fuzzColumnLikeExpressionList(fn->arguments.get());
        fuzzColumnLikeExpressionList(fn->parameters.get());

        /// fuzzColumnLikeExpressionList may remove arguments
        const size_t nargs = fn->arguments ? fn->arguments->children.size() : 0;

        if (nargs == 2 && fuzz_rand() % 30 == 0 && cast_functions.contains(fn->name))
        {
            /// Fuzz casts
            const auto old_type = DataTypeFactory::instance().tryGet(fn->arguments->children[1]);
            const auto new_type = old_type ? fuzzDataType(old_type) : getRandomType();

            fn->arguments->children[1] = make_intrusive<ASTLiteral>(new_type->getName());
        }
        else if (AggregateUtils::isAggregateFunction(*fn))
        {
            if (nargs > 0)
            {
                if (nargs < 3 && fuzz_rand() % 30 == 0)
                {
                    /// Replace aggregate function
                    const Strings & commonAggrs = swapAggrs.at(nargs);

                    for (const auto & entry : commonAggrs)
                    {
                        if (startsWith(fn->name, entry))
                        {
                            String nfname = pickRandomly(fuzz_rand, commonAggrs);
                            /// keep modifiers
                            nfname += fn->name.substr(entry.length(), fn->name.size() - entry.length());

                            fn->name = nfname;
                            break;
                        }
                    }
                }
                if (fuzz_rand() % 30 == 0)
                {
                    /// Add or remove distinct to aggregate
                    static const String distinctSuffix = "Distinct";

                    if (endsWith(fn->name, distinctSuffix))
                    {
                        fn->name = fn->name.substr(0, fn->name.length() - distinctSuffix.size());
                    }
                    else
                    {
                        fn->name = fn->name + distinctSuffix;
                    }
                }
            }
            fn->setNullsAction(fuzzNullsAction(fn->getNullsAction()));
        }
        else if (fuzz_rand() % 30 == 0)
        {
            /// Swap function name
            for (const auto & entry : swapFuncs)
            {
                if (entry.contains(fn->name))
                {
                    fn->name = pickRandomly(fuzz_rand, entry);
                    break;
                }
            }
        }

        if (fn->isWindowFunction() && fn->window_definition)
        {
            auto & def = fn->window_definition->as<ASTWindowDefinition &>();
            fuzzWindowDefinition(def);
        }

        fuzz(fn->children);
    }
    else if (auto * aj = typeid_cast<ASTArrayJoin *>(ast.get()))
    {
        fuzzColumnLikeExpressionList(aj->expression_list.get());

        if (fuzz_rand() % 30 == 0)
        {
            aj->kind = aj->kind == ASTArrayJoin::Kind::Inner ? ASTArrayJoin::Kind::Left : ASTArrayJoin::Kind::Inner;
        }
    }
    else if (auto * tj = typeid_cast<ASTTableJoin *>(ast.get()))
    {
        if (fuzz_rand() % 30 == 0)
        {
            fuzzJoinType(tj);
        }
        if (fuzz_rand() % 20 == 0)
        {
            if (tj->using_expression_list)
            {
                fuzzColumnLikeExpressionList(tj->using_expression_list.get());
            }
            else if (tj->on_expression)
            {
                const ASTPtr & original_on_expression = tj->on_expression;
                const ASTPtr new_on_expression = permutePredicateClause(original_on_expression, 30);

                if (new_on_expression != original_on_expression)
                {
                    tj->children = {new_on_expression};
                    tj->on_expression = tj->children.back();
                }
            }
        }
    }
    else if (auto * select = typeid_cast<ASTSelectQuery *>(ast.get()))
    {
        fuzzColumnLikeExpressionList(select->select().get());

        if (fuzz_rand() % 50 == 0)
        {
            select->distinct = !select->distinct;
        }

        /// Fuzz WITH (CTE) clause
        if (select->with())
        {
            if (fuzz_rand() % 50 == 0)
            {
                /// Drop the entire WITH clause
                select->setExpression(ASTSelectQuery::Expression::WITH, {});
            }
            else
            {
                if (fuzz_rand() % 200 == 0)
                    select->recursive_with = !select->recursive_with;
                /// Remove a random CTE element if multiple exist
                auto & with_children = select->with()->children;
                if (with_children.size() > 1 && fuzz_rand() % 50 == 0)
                    with_children.erase(with_children.begin() + fuzz_rand() % with_children.size());
            }
        }
        if (select->tables().get())
        {
            ASTPtr arr_join;
            ASTPtr new_join;
            const int next_action = fuzz_rand() % 50;

            /// Add a join or remove a table only when tables in FROM are already present
            if (next_action == 0 && !select->refTables()->children.empty() && (new_join = addJoinClause()))
            {
                select->refTables()->children.emplace_back(new_join);
            }
            else if (next_action == 1 && select->refTables()->children.size() > 1)
            {
                auto & children = select->refTables()->children;

                /// Don't remove first FROM table
                children.erase(children.begin() + (fuzz_rand() % (children.size() - 1)) + 1);
            }
            /// Add array join
            if (fuzz_rand() % 50 == 0 && !select->refTables()->children.empty() && (arr_join = addArrayJoinClause()))
            {
                select->refTables()->children.emplace_back(arr_join);
            }
        }
        if (select->groupBy().get())
        {
            if (fuzz_rand() % 50 == 0)
            {
                select->groupBy()->children.clear();
                select->setExpression(ASTSelectQuery::Expression::GROUP_BY, {});
                select->group_by_with_grouping_sets = false;
                select->group_by_with_rollup = false;
                select->group_by_with_cube = false;
                select->group_by_with_totals = true;
            }
            else if (fuzz_rand() % 100 == 0)
            {
                select->group_by_with_grouping_sets = !select->group_by_with_grouping_sets;
            }
            else if (fuzz_rand() % 100 == 0)
            {
                select->group_by_with_rollup = !select->group_by_with_rollup;
            }
            else if (fuzz_rand() % 100 == 0)
            {
                select->group_by_with_cube = !select->group_by_with_cube;
            }
            else if (fuzz_rand() % 100 == 0)
            {
                select->group_by_with_totals = !select->group_by_with_totals;
            }
            if (select->groupBy().get() && !select->groupBy()->children.empty() && fuzz_rand() % 20 == 0)
            {
                /// Permute GROUP BY list
                auto * list = assert_cast<ASTExpressionList *>(select->groupBy().get());

                std::shuffle(list->children.begin(), list->children.end(), fuzz_rand);
            }
            if (select->having().get())
            {
                if (fuzz_rand() % 50 == 0)
                {
                    addOrReplacePredicate(select, ASTSelectQuery::Expression::HAVING);
                }
            }
            else if (fuzz_rand() % 50 == 0)
            {
                addOrReplacePredicate(select, ASTSelectQuery::Expression::HAVING);
            }
        }
        else if (fuzz_rand() % 50 == 0)
        {
            select->setExpression(ASTSelectQuery::Expression::GROUP_BY, getRandomExpressionList(select->select()->children.size()));
        }

        if (select->where().get())
        {
            if (fuzz_rand() % 50 == 0)
            {
                addOrReplacePredicate(select, ASTSelectQuery::Expression::WHERE);
            }
            else if (!select->prewhere().get())
            {
                if (fuzz_rand() % 50 == 0)
                {
                    select->setExpression(ASTSelectQuery::Expression::PREWHERE, select->where()->clone());

                    if (fuzz_rand() % 2 == 0)
                    {
                        select->where()->children.clear();
                        select->setExpression(ASTSelectQuery::Expression::WHERE, {});
                    }
                }
            }
        }
        else if (fuzz_rand() % 50 == 0)
        {
            addOrReplacePredicate(select, ASTSelectQuery::Expression::WHERE);
        }

        if (select->prewhere().get())
        {
            if (fuzz_rand() % 50 == 0)
            {
                addOrReplacePredicate(select, ASTSelectQuery::Expression::PREWHERE);
            }
            else if (!select->where().get())
            {
                if (fuzz_rand() % 50 == 0)
                {
                    select->setExpression(ASTSelectQuery::Expression::WHERE, select->prewhere()->clone());

                    if (fuzz_rand() % 2 == 0)
                    {
                        select->prewhere()->children.clear();
                        select->setExpression(ASTSelectQuery::Expression::PREWHERE, {});
                    }
                }
            }
        }
        else if (fuzz_rand() % 50 == 0)
        {
            addOrReplacePredicate(select, ASTSelectQuery::Expression::PREWHERE);
        }

        if (select->qualify().get())
        {
            if (fuzz_rand() % 50 == 0)
            {
                addOrReplacePredicate(select, ASTSelectQuery::Expression::QUALIFY);
            }
        }
        else if (fuzz_rand() % 50 == 0)
        {
            addOrReplacePredicate(select, ASTSelectQuery::Expression::QUALIFY);
        }

        if (select->orderBy().get())
        {
            if (fuzz_rand() % 50 == 0)
            {
                select->orderBy()->children.clear();
                select->setExpression(ASTSelectQuery::Expression::ORDER_BY, {});
                select->order_by_all = false;
            }
            else
            {
                if (fuzz_rand() % 50 == 0)
                {
                    select->order_by_all = !select->order_by_all;
                }
                fuzzOrderByList(select->orderBy().get(), select->select()->children.size());
            }
        }
        if (select->limitLength())
        {
            if (fuzz_rand() % 50 == 0)
                select->limit_with_ties = !select->limit_with_ties;
            /// Occasionally drop LIMIT (and OFFSET too)
            if (fuzz_rand() % 50 == 0)
            {
                select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, {});
                select->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, {});
            }
            else
            {
                /// Add/remove LIMIT OFFSET
                if (select->limitOffset())
                {
                    if (fuzz_rand() % 50 == 0)
                        select->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, {});
                }
                else if (fuzz_rand() % 50 == 0)
                {
                    select->setExpression(
                        ASTSelectQuery::Expression::LIMIT_OFFSET, make_intrusive<ASTLiteral>(static_cast<UInt64>(fuzz_rand() % 1001)));
                }
            }
        }
        else if (fuzz_rand() % 50 == 0)
        {
            /// Add a LIMIT clause
            select->setExpression(
                ASTSelectQuery::Expression::LIMIT_LENGTH, make_intrusive<ASTLiteral>(static_cast<UInt64>(fuzz_rand() % 1001)));
        }
        /// Fuzz LIMIT BY offset/length
        if (select->limitBy())
        {
            if (select->limitByLength())
            {
                if (fuzz_rand() % 50 == 0)
                    select->setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, {});
            }
            else if (fuzz_rand() % 50 == 0)
            {
                select->setExpression(
                    ASTSelectQuery::Expression::LIMIT_BY_LENGTH, make_intrusive<ASTLiteral>(static_cast<UInt64>(fuzz_rand() % 1001)));
            }
            if (select->limitByOffset())
            {
                if (fuzz_rand() % 50 == 0)
                    select->setExpression(ASTSelectQuery::Expression::LIMIT_BY_OFFSET, {});
            }
            else if (fuzz_rand() % 50 == 0)
            {
                select->setExpression(
                    ASTSelectQuery::Expression::LIMIT_BY_OFFSET, make_intrusive<ASTLiteral>(static_cast<UInt64>(fuzz_rand() % 1001)));
            }
        }
        fuzzColumnLikeExpressionList(select->limitBy().get());

        /// Fuzz WINDOW clause
        if (select->window())
        {
            auto & window_children = select->window()->children;
            if (!window_children.empty() && fuzz_rand() % 100 == 0)
            {
                /// Drop the entire WINDOW clause
                select->setExpression(ASTSelectQuery::Expression::WINDOW, {});
            }
            else if (window_children.size() > 1 && fuzz_rand() % 50 == 0)
            {
                /// Remove one window definition (exercises dangling window-name references)
                window_children.erase(window_children.begin() + fuzz_rand() % window_children.size());
            }
        }

        /// Fuzz inline SETTINGS clause
        if (select->settings())
        {
            if (fuzz_rand() % 100 == 0)
                select->setExpression(ASTSelectQuery::Expression::SETTINGS, {});
        }

        /// Fuzz INTERPOLATE clause (used with ORDER BY ... WITH FILL)
        if (select->interpolate())
        {
            if (fuzz_rand() % 100 == 0)
                select->setExpression(ASTSelectQuery::Expression::INTERPOLATE, {});
            else
            {
                auto & interpolate_children = select->interpolate()->children;
                if (interpolate_children.size() > 1 && fuzz_rand() % 50 == 0)
                    interpolate_children.erase(interpolate_children.begin() + fuzz_rand() % interpolate_children.size());
            }
        }

        fuzz(select->children);
    }
    else if (auto * set = typeid_cast<ASTSetQuery *>(ast.get()))
    {
        /// Fuzz settings
        for (auto & c : set->changes)
            if (fuzz_rand() % 50 == 0)
                c.value = fuzzField(c.value);
    }
    else if (auto * literal = typeid_cast<ASTLiteral *>(ast.get()))
    {
        // There is a caveat with fuzzing the children: many ASTs also keep the
        // links to particular children in own fields. This means that replacing
        // the child with another object might lead to error. Many of these fields
        // are ASTPtr -- this is redundant ownership, but hides the error if the
        // child field is replaced. Others can be ASTLiteral * or the like, which
        // leads to segfault if the pointed-to AST is replaced.
        // Replacing children is safe in case of ASTExpressionList (done in fuzzExpressionList). In a more
        // general case, we can change the value of ASTLiteral, which is what we do here
        if (fuzz_rand() % 11 == 0)
        {
            literal->value = fuzzField(literal->value);
        }
    }
    else if (auto * create_query = typeid_cast<ASTCreateQuery *>(ast.get()))
    {
        /// Toggle CREATE ↔ ATTACH to exercise the attach path with the same schema
        if (fuzz_rand() % 100 == 0)
        {
            create_query->attach = !create_query->attach;
            /// `FROM 'path'` is only valid for ATTACH, not CREATE
            if (!create_query->attach)
            {
                create_query->has_attach_from_path = false;
                create_query->attach_from_path.clear();
            }
        }
        fuzzCreateQuery(*create_query);
    }
    else if (auto * drop_query = typeid_cast<ASTDropQuery *>(ast.get()))
    {
        /// Cycle between DROP / DETACH / TRUNCATE
        if (fuzz_rand() % 100 == 0)
            drop_query->kind = static_cast<ASTDropQuery::Kind>(fuzz_rand() % 3);
        /// DETACH PERMANENTLY: object survives server restart without re-attach
        if (drop_query->kind == ASTDropQuery::Detach && fuzz_rand() % 30 == 0)
            drop_query->permanently = !drop_query->permanently;
        /// DROP SYNC: wait for all mutations/merges to finish before returning
        if (drop_query->kind == ASTDropQuery::Drop && fuzz_rand() % 20 == 0)
            drop_query->sync = !drop_query->sync;
    }
    else if (auto * insert_query = typeid_cast<ASTInsertQuery *>(ast.get()))
    {
        /// Remove a column from the explicit column list (exercises DEFAULT filling)
        if (insert_query->columns && insert_query->columns->children.size() > 1 && fuzz_rand() % 100 == 0)
        {
            insert_query->columns->children.erase(
                insert_query->columns->children.begin() + fuzz_rand() % insert_query->columns->children.size());
        }
        /// Swap format string — only safe when there is no inline data
        if (!insert_query->hasInlinedData() && !insert_query->format.empty() && fuzz_rand() % 20 == 0)
        {
            static const Strings insert_fmts = {
                "Values",
                "CSV",
                "TSV",
                "TabSeparated",
                "TabSeparatedRaw",
                "TSKV",
                "JSON",
                "JSONCompact",
                "JSONColumns",
                "JSONColumnsWithMetadata",
                "JSONEachRow",
                "JSONStringsEachRow",
                "JSONLines",
                "JSONCompactEachRow",
                "JSONCompactStringsEachRow",
                "JSONObjectEachRow",
                "JSONCompactColumns",
                "BSONEachRow",
                "Native",
                "RowBinary",
                "MsgPack",
                "LineAsString",
                "RawBLOB",
                "Parquet",
                "Arrow",
                "ArrowStream",
                "ORC",
                "Avro",
            };
            insert_query->format = insert_fmts[fuzz_rand() % insert_fmts.size()];
        }
        /// Occasionally drop inline SETTINGS
        if (insert_query->settings_ast && fuzz_rand() % 100 == 0)
        {
            auto & ch = insert_query->children;
            ch.erase(std::remove(ch.begin(), ch.end(), insert_query->settings_ast), ch.end());
            insert_query->settings_ast = {};
        }
        fuzz(insert_query->children);
    }
    else if (auto * delete_query = typeid_cast<ASTDeleteQuery *>(ast.get()))
    {
        fuzzMandatoryPredicate(delete_query->predicate, delete_query->children);
        fuzz(delete_query->children);
    }
    else if (auto * update_query = typeid_cast<ASTUpdateQuery *>(ast.get()))
    {
        fuzzMandatoryPredicate(update_query->predicate, update_query->children);
        /// Fuzz SET assignments: remove a column assignment if multiple exist
        if (update_query->assignments && update_query->assignments->children.size() > 1 && fuzz_rand() % 50 == 0)
        {
            update_query->assignments->children.erase(
                update_query->assignments->children.begin() + fuzz_rand() % update_query->assignments->children.size());
        }
        fuzz(update_query->children);
    }
    else if (auto * alter_query = typeid_cast<ASTAlterQuery *>(ast.get()))
    {
        /// Remove a random command from a multi-command ALTER (exercises partial-ALTER paths)
        if (alter_query->command_list && alter_query->command_list->children.size() > 1 && fuzz_rand() % 100 == 0)
        {
            auto & cmds = alter_query->command_list->children;
            cmds.erase(cmds.begin() + fuzz_rand() % cmds.size());
        }
        fuzz(alter_query->children);
    }
    else if (auto * alter_cmd = typeid_cast<ASTAlterCommand *>(ast.get()))
    {
        switch (alter_cmd->type)
        {
            case ASTAlterCommand::DELETE:
            case ASTAlterCommand::UPDATE:
                /// WHERE predicate is mandatory — replace or permute, never remove
                if (alter_cmd->predicate)
                {
                    for (auto & child : alter_cmd->children)
                    {
                        if (child.get() == alter_cmd->predicate)
                        {
                            fuzzMandatoryPredicate(child, alter_cmd->children);
                            alter_cmd->predicate = child.get();
                            break;
                        }
                    }
                }
                if (alter_cmd->type == ASTAlterCommand::UPDATE && alter_cmd->update_assignments)
                {
                    auto & asgns = alter_cmd->update_assignments->children;
                    if (asgns.size() > 1 && fuzz_rand() % 50 == 0)
                        asgns.erase(asgns.begin() + fuzz_rand() % asgns.size());
                }
                break;
            case ASTAlterCommand::ADD_COLUMN:
            case ASTAlterCommand::MODIFY_COLUMN:
                /// fuzzColumnDeclaration is normally only called from fuzzCreateQuery;
                /// exercise it here too so ALTER ADD/MODIFY COLUMN paths get the same coverage
                if (alter_cmd->col_decl)
                    if (auto * col = alter_cmd->col_decl->as<ASTColumnDeclaration>())
                        fuzzColumnDeclaration(*col);
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->first = !alter_cmd->first;
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->if_not_exists = !alter_cmd->if_not_exists;
                break;
            case ASTAlterCommand::DROP_COLUMN:
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->clear_column = !alter_cmd->clear_column;
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->if_exists = !alter_cmd->if_exists;
                break;
            case ASTAlterCommand::ADD_INDEX:
                /// Apply the same index-specific mutations as fuzzCreateQuery does for CREATE TABLE
                if (alter_cmd->index_decl)
                    if (auto * idx = alter_cmd->index_decl->as<ASTIndexDeclaration>())
                        fuzzIndexDeclaration(*idx);
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->first = !alter_cmd->first;
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->if_not_exists = !alter_cmd->if_not_exists;
                break;
            case ASTAlterCommand::DROP_INDEX:
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->clear_index = !alter_cmd->clear_index;
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->if_exists = !alter_cmd->if_exists;
                break;
            case ASTAlterCommand::ADD_CONSTRAINT:
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->if_not_exists = !alter_cmd->if_not_exists;
                break;
            case ASTAlterCommand::ADD_PROJECTION:
                /// Apply the same projection-specific mutations as fuzzCreateQuery
                if (alter_cmd->projection_decl)
                    if (auto * proj = alter_cmd->projection_decl->as<ASTProjectionDeclaration>())
                        fuzzProjectionDeclaration(*proj);
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->if_not_exists = !alter_cmd->if_not_exists;
                break;
            case ASTAlterCommand::DROP_PROJECTION:
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->clear_projection = !alter_cmd->clear_projection;
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->if_exists = !alter_cmd->if_exists;
                break;
            case ASTAlterCommand::ADD_STATISTICS:
            case ASTAlterCommand::MODIFY_STATISTICS: {
                /// Fuzz stat types the same way fuzzColumnDeclaration does
                static const Strings stat_types = {"tdigest", "countmin", "minmax", "uniq"};
                if (alter_cmd->statistics_decl)
                    if (auto * stats = alter_cmd->statistics_decl->as<ASTStatisticsDeclaration>())
                        if (stats->types)
                            for (auto & type_ast : stats->types->children)
                                if (auto * afn = type_ast->as<ASTFunction>(); afn && fuzz_rand() % 5 == 0)
                                    afn->name = pickRandomly(fuzz_rand, stat_types);
                break;
            }
            case ASTAlterCommand::DROP_STATISTICS:
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->clear_statistics = !alter_cmd->clear_statistics;
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->if_exists = !alter_cmd->if_exists;
                break;
            case ASTAlterCommand::MODIFY_SETTING:
            case ASTAlterCommand::MODIFY_DATABASE_SETTING:
                /// Fuzz individual setting values (same strategy as ASTSetQuery handler)
                if (alter_cmd->settings_changes)
                    if (auto * aset = alter_cmd->settings_changes->as<ASTSetQuery>())
                        for (auto & c : aset->changes)
                            if (fuzz_rand() % 50 == 0)
                                c.value = fuzzField(c.value);
                break;
            case ASTAlterCommand::RESET_SETTING:
                /// Occasionally drop a setting name from the reset list
                if (alter_cmd->settings_resets && alter_cmd->settings_resets->children.size() > 1 && fuzz_rand() % 20 == 0)
                {
                    auto & resets = alter_cmd->settings_resets->children;
                    resets.erase(resets.begin() + fuzz_rand() % resets.size());
                }
                break;
            case ASTAlterCommand::REPLACE_PARTITION:
                /// Toggle between REPLACE PARTITION FROM and ATTACH PARTITION FROM
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->replace = !alter_cmd->replace;
                break;
            case ASTAlterCommand::DROP_PARTITION:
            case ASTAlterCommand::ATTACH_PARTITION:
            case ASTAlterCommand::DROP_DETACHED_PARTITION:
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->detach = !alter_cmd->detach;
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->part = !alter_cmd->part;
                break;
            case ASTAlterCommand::MOVE_PARTITION:
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->detach = !alter_cmd->detach;
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->part = !alter_cmd->part;
                /// Cycle move destination type between DISK, VOLUME, TABLE
                if (fuzz_rand() % 10 == 0)
                {
                    static const DataDestinationType dest_types[] = {
                        DataDestinationType::DISK,
                        DataDestinationType::VOLUME,
                        DataDestinationType::TABLE,
                    };
                    alter_cmd->move_destination_type = dest_types[fuzz_rand() % 3];
                }
                break;
            case ASTAlterCommand::DROP_CONSTRAINT:
            case ASTAlterCommand::COMMENT_COLUMN:
                if (fuzz_rand() % 20 == 0)
                    alter_cmd->if_exists = !alter_cmd->if_exists;
                break;
            default:
                break;
        }
        fuzz(alter_cmd->children);
    }
    else if (auto * optimize_query = typeid_cast<ASTOptimizeQuery *>(ast.get()))
    {
        if (fuzz_rand() % 20 == 0)
        {
            optimize_query->final = !optimize_query->final;
        }
        if (fuzz_rand() % 20 == 0)
        {
            optimize_query->deduplicate = !optimize_query->deduplicate;
        }
        if (fuzz_rand() % 20 == 0)
        {
            optimize_query->cleanup = !optimize_query->cleanup;
        }
        if (optimize_query->deduplicate_by_columns && fuzz_rand() % 20 == 0)
        {
            fuzz(optimize_query->deduplicate_by_columns);
        }
    }
    else if (auto * explain_query = typeid_cast<ASTExplainQuery *>(ast.get()))
    {
        const auto & explained_query = explain_query->getExplainedQuery();
        /// Fuzzing EXPLAIN query to SELECT query randomly
        if (explained_query && explained_query->getQueryKind() == IAST::QueryKind::Select && fuzz_rand() % 20 == 0)
        {
            auto select_query = explain_query->getExplainedQuery()->clone();
            fuzz(select_query);
            ast = select_query;
        }
        else
        {
            fuzzExplainQuery(*explain_query);
        }
    }
    else if (auto * wdef = typeid_cast<ASTWindowDefinition *>(ast.get()))
    {
        fuzzWindowDefinition(*wdef);
        fuzz(wdef->children);
    }
    else if (auto * with_elem = typeid_cast<ASTWithElement *>(ast.get()))
    {
        /// Occasionally rename the CTE to exercise unresolved-reference paths
        if (fuzz_rand() % 200 == 0)
            with_elem->name = "cte_" + std::to_string(fuzz_rand() % 10);
        fuzz(with_elem->children);
    }
    else if (auto * interpolate_elem = typeid_cast<ASTInterpolateElement *>(ast.get()))
    {
        /// Occasionally rename the target column to exercise unresolved-column paths
        if (fuzz_rand() % 200 == 0)
            interpolate_elem->column = "col_" + std::to_string(fuzz_rand() % 10);
        fuzz(interpolate_elem->children);
    }
    else if (auto * sample_ratio = typeid_cast<ASTSampleRatio *>(ast.get()))
    {
        if (fuzz_rand() % 20 == 0)
        {
            static const std::vector<UInt64> sample_values = {1, 2, 5, 10, 100, 1000, 10000};
            sample_ratio->ratio.numerator = sample_values[fuzz_rand() % sample_values.size()];
        }
        if (fuzz_rand() % 20 == 0)
        {
            static const std::vector<UInt64> denom_values = {1, 2, 5, 10, 100, 1000, 10000};
            sample_ratio->ratio.denominator = denom_values[fuzz_rand() % denom_values.size()];
        }
    }
    else if (auto * apply = typeid_cast<ASTColumnsApplyTransformer *>(ast.get()))
    {
        if (fuzz_rand() % 50 == 0)
            apply->column_name_prefix = fuzz_rand() % 2 == 0 ? "" : "f_";
        fuzz(apply->children);
    }
    else if (auto * except_transformer = typeid_cast<ASTColumnsExceptTransformer *>(ast.get()))
    {
        if (fuzz_rand() % 50 == 0)
            except_transformer->is_strict = !except_transformer->is_strict;
        fuzz(except_transformer->children);
    }
    else if (auto * replace_transformer = typeid_cast<ASTColumnsReplaceTransformer *>(ast.get()))
    {
        if (fuzz_rand() % 50 == 0)
            replace_transformer->is_strict = !replace_transformer->is_strict;
        fuzz(replace_transformer->children);
    }
    else if (auto * create_function = typeid_cast<ASTCreateSQLFunctionQuery *>(ast.get()))
    {
        /// Toggle OR REPLACE / IF NOT EXISTS flags to exercise all creation paths
        if (fuzz_rand() % 50 == 0)
            create_function->or_replace = !create_function->or_replace;
        if (fuzz_rand() % 50 == 0)
            create_function->if_not_exists = !create_function->if_not_exists;
        /// Fuzz the lambda expression body
        fuzz(create_function->children);
    }
    else if (auto * system_query = typeid_cast<ASTSystemQuery *>(ast.get()))
    {
        using Type = ASTSystemQuery::Type;
        /// Toggle paired commands to exercise both code paths with the same operand.
        /// Includes START/STOP pairs, individual/bulk reload pairs, and ENABLE/DISABLE pairs.
        if (fuzz_rand() % 20 == 0)
        {
            static const std::pair<Type, Type> toggleable_pairs[] = {
                {Type::STOP_MERGES, Type::START_MERGES},
                {Type::STOP_TTL_MERGES, Type::START_TTL_MERGES},
                {Type::STOP_FETCHES, Type::START_FETCHES},
                {Type::STOP_MOVES, Type::START_MOVES},
                {Type::STOP_REPLICATED_SENDS, Type::START_REPLICATED_SENDS},
                {Type::STOP_REPLICATION_QUEUES, Type::START_REPLICATION_QUEUES},
                {Type::STOP_REPLICATED_DDL_QUERIES, Type::START_REPLICATED_DDL_QUERIES},
                {Type::STOP_DISTRIBUTED_SENDS, Type::START_DISTRIBUTED_SENDS},
                {Type::STOP_THREAD_FUZZER, Type::START_THREAD_FUZZER},
                {Type::STOP_PULLING_REPLICATION_LOG, Type::START_PULLING_REPLICATION_LOG},
                {Type::STOP_CLEANUP, Type::START_CLEANUP},
                {Type::STOP_VIEW, Type::START_VIEW},
                {Type::STOP_VIEWS, Type::START_VIEWS},
                {Type::STOP_REPLICATED_VIEW, Type::START_REPLICATED_VIEW},
                {Type::STOP_VIRTUAL_PARTS_UPDATE, Type::START_VIRTUAL_PARTS_UPDATE},
                {Type::STOP_REDUCE_BLOCKING_PARTS, Type::START_REDUCE_BLOCKING_PARTS},
                {Type::STOP_LISTEN, Type::START_LISTEN},
                {Type::LOAD_PRIMARY_KEY, Type::UNLOAD_PRIMARY_KEY},
                /* These are too slow
                {Type::RELOAD_FUNCTION, Type::RELOAD_FUNCTIONS},
                {Type::RELOAD_DICTIONARY, Type::RELOAD_DICTIONARIES},
                {Type::RELOAD_MODEL, Type::RELOAD_MODELS},*/
                {Type::JEMALLOC_ENABLE_PROFILE, Type::JEMALLOC_DISABLE_PROFILE},
                {Type::JEMALLOC_PURGE, Type::JEMALLOC_FLUSH_PROFILE},
                {Type::FLUSH_LOGS, Type::FLUSH_ASYNC_INSERT_QUEUE},
                {Type::ALLOCATE_MEMORY, Type::FREE_MEMORY},
                {Type::RESTART_REPLICA, Type::RESTORE_REPLICA},
                {Type::RESTART_DISK, Type::CLEAR_DISK_METADATA_CACHE},
            };
            for (const auto & [a_type, b_type] : toggleable_pairs)
            {
                if (system_query->type == a_type)
                {
                    system_query->type = b_type;
                    break;
                }
                else if (system_query->type == b_type)
                {
                    system_query->type = a_type;
                    break;
                }
            }
        }
        /// Toggle IF EXISTS
        if (fuzz_rand() % 20 == 0)
            system_query->if_exists = !system_query->if_exists;
        /// Cycle through SYNC REPLICA / SYNC DATABASE REPLICA modes
        if ((system_query->type == Type::SYNC_REPLICA || system_query->type == Type::SYNC_DATABASE_REPLICA) && fuzz_rand() % 5 == 0)
        {
            static const SyncReplicaMode sync_modes[] = {
                SyncReplicaMode::DEFAULT,
                SyncReplicaMode::STRICT,
                SyncReplicaMode::LIGHTWEIGHT,
                SyncReplicaMode::PULL,
            };
            system_query->sync_replica_mode = sync_modes[fuzz_rand() % std::size(sync_modes)];
        }
        /// Fuzz SUSPEND duration — try zero, boundary, and large values
        if (system_query->type == Type::SUSPEND && fuzz_rand() % 5 == 0)
        {
            static const UInt64 suspend_seconds[] = {0, 1, 2, 5, 10, 3600};
            system_query->seconds = suspend_seconds[fuzz_rand() % std::size(suspend_seconds)];
        }
        /// Rotate among view commands that all take a table argument
        {
            static const Type view_cmd_types[] = {
                Type::REFRESH_VIEW,
                Type::START_VIEW,
                Type::START_REPLICATED_VIEW,
                Type::STOP_VIEW,
                Type::STOP_REPLICATED_VIEW,
                Type::CANCEL_VIEW,
                Type::WAIT_VIEW,
            };
            for (const auto & t : view_cmd_types)
            {
                if (system_query->type == t && fuzz_rand() % 10 == 0)
                {
                    system_query->type = view_cmd_types[fuzz_rand() % std::size(view_cmd_types)];
                    break;
                }
            }
        }
        /// Rotate among no-argument SYNC commands
        {
            static const Type sync_types[] = {
                Type::SYNC_TRANSACTION_LOG,
                Type::SYNC_FILE_CACHE,
                Type::SYNC_FILESYSTEM_CACHE,
            };
            for (const auto & t : sync_types)
            {
                if (system_query->type == t && fuzz_rand() % 10 == 0)
                {
                    system_query->type = sync_types[fuzz_rand() % std::size(sync_types)];
                    break;
                }
            }
        }
        /// Cycle WAIT FAILPOINT action between PAUSE / RESUME / UNSPECIFIED
        if (system_query->type == Type::WAIT_FAILPOINT && fuzz_rand() % 5 == 0)
        {
            static const ASTSystemQuery::FailPointAction fp_actions[] = {
                ASTSystemQuery::FailPointAction::UNSPECIFIED,
                ASTSystemQuery::FailPointAction::PAUSE,
                ASTSystemQuery::FailPointAction::RESUME,
            };
            system_query->fail_point_action = fp_actions[fuzz_rand() % std::size(fp_actions)];
        }
        /// Rotate among failpoint commands that all take a fail_point_name argument
        if ((system_query->type == Type::ENABLE_FAILPOINT || system_query->type == Type::DISABLE_FAILPOINT
             || system_query->type == Type::NOTIFY_FAILPOINT || system_query->type == Type::WAIT_FAILPOINT)
            && fuzz_rand() % 10 == 0)
        {
            static const Type failpoint_types[] = {
                Type::ENABLE_FAILPOINT,
                Type::DISABLE_FAILPOINT,
                Type::NOTIFY_FAILPOINT,
                Type::WAIT_FAILPOINT,
            };
            system_query->type = failpoint_types[fuzz_rand() % std::size(failpoint_types)];
        }
        /// Toggle TEST VIEW between SET FAKE TIME and UNSET FAKE TIME
        if (system_query->type == Type::TEST_VIEW && fuzz_rand() % 5 == 0)
        {
            if (system_query->fake_time_for_view.has_value())
                system_query->fake_time_for_view.reset();
            else
                system_query->fake_time_for_view = static_cast<Int64>(fuzz_rand() % 2000000000LL);
        }
        /// Toggle CLEAR FILESYSTEM CACHE between clearing all, by name, and by name+key+offset
        if (system_query->type == Type::CLEAR_FILESYSTEM_CACHE && fuzz_rand() % 5 == 0)
        {
            if (system_query->filesystem_cache_name.empty())
            {
                system_query->filesystem_cache_name = "default";
            }
            else if (system_query->key_to_drop.empty())
            {
                system_query->key_to_drop = std::to_string(fuzz_rand());
            }
            else if (!system_query->offset_to_drop.has_value())
            {
                system_query->offset_to_drop = fuzz_rand() % 1024;
            }
            else
            {
                /// Reset back to clearing all
                system_query->filesystem_cache_name.clear();
                system_query->key_to_drop.clear();
                system_query->offset_to_drop.reset();
            }
        }
        /// Toggle CLEAR DISTRIBUTED CACHE between drop-connections mode and server-id mode
        if (system_query->type == Type::CLEAR_DISTRIBUTED_CACHE && fuzz_rand() % 5 == 0)
        {
            system_query->distributed_cache_drop_connections = !system_query->distributed_cache_drop_connections;
            if (!system_query->distributed_cache_drop_connections && system_query->distributed_cache_server_id.empty())
                system_query->distributed_cache_server_id = "server_" + std::to_string(fuzz_rand() % 4);
        }
        /// Toggle CLEAR SCHEMA CACHE optional storage specifier
        if (system_query->type == Type::CLEAR_SCHEMA_CACHE && fuzz_rand() % 5 == 0)
        {
            if (system_query->schema_cache_storage.empty())
            {
                static const String storages[] = {"S3", "HDFS", "URL", "File", "AzureBlobStorage"};
                system_query->schema_cache_storage = storages[fuzz_rand() % std::size(storages)];
            }
            else
                system_query->schema_cache_storage.clear();
        }
        /// Toggle CLEAR FORMAT SCHEMA CACHE optional format specifier
        if (system_query->type == Type::CLEAR_FORMAT_SCHEMA_CACHE && fuzz_rand() % 5 == 0)
        {
            if (system_query->schema_cache_format.empty())
            {
                static const String formats[] = {"Protobuf", "CapnProto", "FlatBuffers", "MsgPack"};
                system_query->schema_cache_format = formats[fuzz_rand() % std::size(formats)];
            }
            else
                system_query->schema_cache_format.clear();
        }
        /// Fuzz ALLOCATE MEMORY size with boundary and large values
        if (system_query->type == Type::ALLOCATE_MEMORY && fuzz_rand() % 5 == 0)
        {
            static const UInt64 mem_sizes[] = {0, 1, 4096, 1ULL << 20, 1ULL << 30};
            system_query->untracked_memory_size = mem_sizes[fuzz_rand() % std::size(mem_sizes)];
        }
        /// Toggle CLEAR QUERY CACHE between clearing all and clearing by tag
        if (system_query->type == Type::CLEAR_QUERY_CACHE && fuzz_rand() % 5 == 0)
        {
            if (system_query->query_result_cache_tag.has_value())
                system_query->query_result_cache_tag.reset();
            else
                system_query->query_result_cache_tag = "fuzz_tag_" + std::to_string(fuzz_rand() % 4);
        }
        /// Rotate among no-argument cache-clear types to exercise different cache subsystems
        /// with the same surrounding query context
        {
            static const Type plain_cache_types[] = {
                Type::CLEAR_DNS_CACHE,
                Type::CLEAR_CONNECTIONS_CACHE,
                Type::CLEAR_MARK_CACHE,
                Type::CLEAR_PRIMARY_INDEX_CACHE,
                Type::CLEAR_UNCOMPRESSED_CACHE,
                Type::CLEAR_INDEX_MARK_CACHE,
                Type::CLEAR_INDEX_UNCOMPRESSED_CACHE,
                Type::CLEAR_VECTOR_SIMILARITY_INDEX_CACHE,
                Type::CLEAR_TEXT_INDEX_TOKENS_CACHE,
                Type::CLEAR_TEXT_INDEX_HEADER_CACHE,
                Type::CLEAR_TEXT_INDEX_POSTINGS_CACHE,
                Type::CLEAR_TEXT_INDEX_CACHES,
                Type::CLEAR_MMAP_CACHE,
                Type::CLEAR_QUERY_CONDITION_CACHE,
                Type::CLEAR_COMPILED_EXPRESSION_CACHE,
                Type::CLEAR_ICEBERG_METADATA_CACHE,
                Type::CLEAR_PAGE_CACHE,
                Type::CLEAR_S3_CLIENT_CACHE,
            };
            for (const auto & t : plain_cache_types)
            {
                if (system_query->type == t && fuzz_rand() % 10 == 0)
                {
                    system_query->type = plain_cache_types[fuzz_rand() % std::size(plain_cache_types)];
                    break;
                }
            }
        }
        /// Toggle between the two prewarm cache types (same optional-table structure)
        if ((system_query->type == Type::PREWARM_MARK_CACHE || system_query->type == Type::PREWARM_PRIMARY_INDEX_CACHE)
            && fuzz_rand() % 5 == 0)
        {
            system_query->type
                = (system_query->type == Type::PREWARM_MARK_CACHE) ? Type::PREWARM_PRIMARY_INDEX_CACHE : Type::PREWARM_MARK_CACHE;
        }
        /// Rotate among no-argument reload commands
        {
            static const Type reload_types[] = {
                Type::RELOAD_CONFIG,
                Type::RELOAD_USERS,
                Type::RELOAD_ASYNCHRONOUS_METRICS,
                Type::RELOAD_EMBEDDED_DICTIONARIES,
            };
            for (const auto & t : reload_types)
            {
                if (system_query->type == t && fuzz_rand() % 10 == 0)
                {
                    system_query->type = reload_types[fuzz_rand() % std::size(reload_types)];
                    break;
                }
            }
        }
        /// Fuzz RELOAD DELTA KERNEL TRACING level string
        if (system_query->type == Type::RELOAD_DELTA_KERNEL_TRACING && fuzz_rand() % 5 == 0)
        {
            static const String tracing_levels[] = {"none", "error", "warning", "information", "debug", "trace"};
            system_query->delta_kernel_tracing_level = tracing_levels[fuzz_rand() % std::size(tracing_levels)];
        }
        /// Fuzz DROP REPLICA optional fields: shard, is_drop_whole_replica, with_tables
        if ((system_query->type == Type::DROP_REPLICA || system_query->type == Type::DROP_DATABASE_REPLICA
             || system_query->type == Type::DROP_CATALOG_REPLICA)
            && fuzz_rand() % 5 == 0)
        {
            const uint32_t choice = fuzz_rand() % 3;
            if (choice == 0)
                system_query->is_drop_whole_replica = !system_query->is_drop_whole_replica;
            else if (choice == 1)
                system_query->with_tables = !system_query->with_tables;
            else
            {
                if (system_query->shard.empty())
                    system_query->shard = "shard_" + std::to_string(fuzz_rand() % 4);
                else
                    system_query->shard.clear();
            }
        }
        fuzz(system_query->children);
    }
    else
    {
        fuzz(ast->children);
    }
}

#define AST_FUZZER_PART_TYPE_CAP 1000

/*
 * This functions collects various parts of query that we can then substitute
 * to a query being fuzzed.
 */
void QueryFuzzer::collectFuzzInfoMain(ASTPtr ast)
{
    collectFuzzInfoRecurse(ast);
}

void QueryFuzzer::addTableLike(ASTPtr ast)
{
    if (table_like_map.size() > AST_FUZZER_PART_TYPE_CAP)
    {
        const auto iter = std::next(table_like.begin(), fuzz_rand() % table_like.size());
        const auto & ast_del = *iter;
        table_like_map.erase(ast_del.first);
        table_like.erase(iter);
    }

    const auto & alias = ast->tryGetAlias();
    const auto & name = alias.empty() ? ast->formatForErrorMessage() : alias;
    if (name.size() < 200)
    {
        const auto res = table_like_map.insert({name, ast});
        if (res.second)
        {
            table_like.push_back({name, ast});
        }
    }
}

void QueryFuzzer::addColumnLike(ASTPtr ast)
{
    if (column_like_map.size() > AST_FUZZER_PART_TYPE_CAP)
    {
        const auto iter = std::next(column_like.begin(), fuzz_rand() % column_like.size());
        const auto & ast_del = *iter;
        column_like_map.erase(ast_del.first);
        column_like.erase(iter);
    }

    const auto & alias = ast->tryGetAlias();
    const auto & name = alias.empty() ? ast->formatForErrorMessage() : alias;
    if (name == "Null")
    {
        // The `Null` identifier from FORMAT Null clause. We don't quote it
        // properly when formatting the AST, and while the resulting query
        // technically works, it has non-standard case for Null (the standard
        // is NULL), so it breaks the query formatting idempotence check.
        // Just plug this particular case for now.
        return;
    }
    if (name.size() < 200)
    {
        const auto res = column_like_map.insert({name, ast});
        if (res.second)
        {
            column_like.push_back({name, ast});
        }
    }
}

void QueryFuzzer::collectFuzzInfoRecurse(ASTPtr ast)
{
    if (typeid_cast<ASTLiteral *>(ast.get()))
    {
        addColumnLike(ast);
    }
    else if (typeid_cast<ASTIdentifier *>(ast.get()))
    {
        addColumnLike(ast);
    }
    else if (const auto * fn = typeid_cast<const ASTFunction *>(ast.get()))
    {
        addColumnLike(ast);
        if (TableFunctionFactory::instance().isTableFunctionName(fn->name))
        {
            addTableLike(ast);
        }
    }
    else if (typeid_cast<ASTTableExpression *>(ast.get()))
    {
        addTableLike(ast);
    }
    else if (typeid_cast<ASTSubquery *>(ast.get()))
    {
        addTableLike(ast);
    }

    for (const auto & child : ast->children)
    {
        collectFuzzInfoRecurse(child);
    }
}

void QueryFuzzer::fuzzMain(ASTPtr & ast)
{
    current_ast_depth = 0;
    iteration_count = 0;
    debug_visited_nodes.clear();
    debug_top_ast = &ast;

    collectFuzzInfoMain(ast);
    fuzz(ast);

    CurrentMetrics::set(CurrentMetrics::ASTFuzzerAccumulatedFragments, getAccumulatedStateSize());

    if (out_stream)
    {
        *out_stream << std::endl;
        *out_stream << ast->formatWithSecretsOneLine() << std::endl << std::endl;
    }
}

}
