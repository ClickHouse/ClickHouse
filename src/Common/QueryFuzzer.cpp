#include <Common/QueryFuzzer.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromOStream.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/ParserQuery.h>
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
            type_index = fuzz_rand() % 3;
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

    ASTPtr new_ast = std::make_shared<ASTExpressionList>();
    for (size_t i = 0; i < fuzz_rand() % 5 + 1; ++i)
    {
        /// Use Group by number in the projection, starting from position 1
        new_ast->children.emplace_back(
            nproj && (fuzz_rand() % 4 == 0) ? std::make_shared<ASTLiteral>((fuzz_rand() % nproj) + 1) : getRandomColumnLike());
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
        auto * pos = list->children.empty() ? list->children.begin() : list->children.begin() + fuzz_rand() % list->children.size();
        const auto col = nproj && (fuzz_rand() % 4 == 0) ? std::make_shared<ASTLiteral>((fuzz_rand() % nproj) + 1) : getRandomColumnLike();
        if (col)
        {
            auto elem = std::make_shared<ASTOrderByElement>();
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
        auto * pos = impl->children.empty() ? impl->children.begin() : impl->children.begin() + fuzz_rand() % impl->children.size();
        auto col = getRandomColumnLike();
        if (col)
            impl->children.insert(pos, col);
        else if (debug_stream)
            *debug_stream << "No random column.\n";
    }

    // We don't have to recurse here to fuzz the children, this is handled by
    // the generic recursion into IAST.children.
}

void QueryFuzzer::fuzzNullsAction(NullsAction & action)
{
    /// If it's not using actions, then it's a high change it doesn't support it to begin with
    if ((action == NullsAction::EMPTY) && (fuzz_rand() % 100 == 0))
    {
        if (fuzz_rand() % 2 == 0)
            action = NullsAction::RESPECT_NULLS;
        else
            action = NullsAction::IGNORE_NULLS;
    }
    else if (fuzz_rand() % 20 == 0)
    {
        switch (fuzz_rand() % 3)
        {
            case 0: {
                action = NullsAction::EMPTY;
                break;
            }
            case 1: {
                action = NullsAction::RESPECT_NULLS;
                break;
            }
            default: {
                action = NullsAction::IGNORE_NULLS;
                break;
            }
        }
    }
}

void QueryFuzzer::fuzzWindowFrame(ASTWindowDefinition & def)
{
    checkIterationLimit();

    switch (fuzz_rand() % 40)
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
                def.frame_begin_offset = std::make_shared<ASTLiteral>(getRandomField(0));
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
                def.frame_end_offset = std::make_shared<ASTLiteral>(getRandomField(0));
            }
            else
            {
                def.frame_end_offset = nullptr;
            }
            break;
        }
        case 5: {
            def.frame_begin_preceding = fuzz_rand() % 2;
            break;
        }
        case 6: {
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

    if (create.storage && create.storage->engine)
    {
        /// Replace ReplicatedMergeTree to ordinary MergeTree
        /// to avoid inconsistency of metadata in zookeeper.
        auto & engine_name = create.storage->engine->name;
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
    if (column.type)
    {
        auto data_type = fuzzDataType(DataTypeFactory::instance().get(column.type));

        ParserDataType parser;
        column.type = parseQuery(
            parser, data_type->getName(), DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
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

        return type_tuple->hasExplicitNames() ? std::make_shared<DataTypeTuple>(elements, type_tuple->getElementNames())
                                              : std::make_shared<DataTypeTuple>(elements);
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

    static const std::vector<TypeIndex> & random_types
        = {TypeIndex::UInt8,       TypeIndex::UInt16,         TypeIndex::UInt32,   TypeIndex::UInt64,     TypeIndex::UInt128,
           TypeIndex::UInt256,     TypeIndex::Int8,           TypeIndex::Int16,    TypeIndex::Int32,      TypeIndex::Int64,
           TypeIndex::Int128,      TypeIndex::Int256,         TypeIndex::BFloat16, TypeIndex::Float32,    TypeIndex::Float64,
           TypeIndex::Date,        TypeIndex::Date32,         TypeIndex::DateTime, TypeIndex::DateTime64, TypeIndex::String,
           TypeIndex::FixedString, TypeIndex::Enum8,          TypeIndex::Enum16,   TypeIndex::Decimal32,  TypeIndex::Decimal64,
           TypeIndex::Decimal128,  TypeIndex::Decimal256,     TypeIndex::UUID,     TypeIndex::Array,      TypeIndex::Tuple,
           TypeIndex::Nullable,    TypeIndex::LowCardinality, TypeIndex::Map,      TypeIndex::IPv4,       TypeIndex::IPv6,
           TypeIndex::Variant,     TypeIndex::Dynamic /*,        TypeIndex::Time,     TypeIndex::Time64*/};
    const auto type_id = pickRandomly(fuzz_rand, random_types);

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
        case TypeIndex::LowCardinality:
            return std::make_shared<DataTypeLowCardinality>(getRandomType());
        case TypeIndex::Nullable:
            return std::make_shared<DataTypeNullable>(getRandomType());
            DISPATCH(Decimal32)
            DISPATCH(Decimal64)
            DISPATCH(Decimal128)
            DISPATCH(Decimal256)
        case TypeIndex::FixedString:
            return std::make_shared<DataTypeFixedString>(fuzz_rand() % 20);
        case TypeIndex::Enum8:
            return std::make_shared<DataTypeUInt8>();
        case TypeIndex::Enum16:
            return std::make_shared<DataTypeUInt16>();
        case TypeIndex::Dynamic:
            return std::make_shared<DataTypeDynamic>(fuzz_rand() % 20);
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
        table.database_and_table_name = std::make_shared<ASTTableIdentifier>(new_table_id);
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
        auto settings_ast = std::make_shared<ASTSetQuery>();
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
    if (fuzz_rand() % 11 == 0)
    {
        return ASTExplainQuery::ExplainKind::ParsedAST;
    }
    if (fuzz_rand() % 11 == 0)
    {
        return ASTExplainQuery::ExplainKind::AnalyzedSyntax;
    }
    if (fuzz_rand() % 11 == 0)
    {
        return ASTExplainQuery::ExplainKind::QueryTree;
    }
    if (fuzz_rand() % 11 == 0)
    {
        return ASTExplainQuery::ExplainKind::QueryPlan;
    }
    if (fuzz_rand() % 11 == 0)
    {
        return ASTExplainQuery::ExplainKind::QueryPipeline;
    }
    if (fuzz_rand() % 11 == 0)
    {
        return ASTExplainQuery::ExplainKind::QueryEstimates;
    }
    if (fuzz_rand() % 11 == 0)
    {
        return ASTExplainQuery::ExplainKind::TableOverride;
    }
    if (fuzz_rand() % 11 == 0)
    {
        return ASTExplainQuery::ExplainKind::CurrentTransaction;
    }
    return kind;
}

void QueryFuzzer::fuzzExplainSettings(ASTSetQuery & settings_ast, ASTExplainQuery::ExplainKind kind)
{
    auto & changes = settings_ast.changes;

    static const std::unordered_map<ASTExplainQuery::ExplainKind, std::vector<String>> settings_by_kind
        = {{ASTExplainQuery::ExplainKind::ParsedAST, {"graph", "optimize"}},
           {ASTExplainQuery::ExplainKind::AnalyzedSyntax, {"oneline", "query_tree_passes"}},
           {ASTExplainQuery::QueryTree, {"run_passes", "dump_passes", "dump_ast", "passes"}},
           {ASTExplainQuery::ExplainKind::QueryPlan, {"header, description", "actions", "indexes", "optimize", "json", "sorting"}},
           {ASTExplainQuery::ExplainKind::QueryPipeline, {"header", "graph=1", "compact"}},
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
            "toFixedString", std::make_shared<ASTLiteral>(value), std::make_shared<ASTLiteral>(static_cast<UInt64>(value.size())));
    }
    else if (type == Field::Types::Which::UInt64 && fuzz_rand() % 7 == 0)
    {
        child = makeASTFunction(fuzz_rand() % 2 == 0 ? "toUInt128" : "toUInt256", std::make_shared<ASTLiteral>(l->value.safeGet<UInt64>()));
    }
    else if (type == Field::Types::Which::Int64 && fuzz_rand() % 7 == 0)
    {
        child = makeASTFunction(fuzz_rand() % 2 == 0 ? "toInt128" : "toInt256", std::make_shared<ASTLiteral>(l->value.safeGet<Int64>()));
    }
    else if (type == Field::Types::Which::Float64 && fuzz_rand() % 7 == 0)
    {
        const int decimal = fuzz_rand() % 4;
        if (decimal == 0)
            child = makeASTFunction(
                "toDecimal32",
                std::make_shared<ASTLiteral>(l->value.safeGet<Float64>()),
                std::make_shared<ASTLiteral>(static_cast<UInt64>(fuzz_rand() % 9)));
        else if (decimal == 1)
            child = makeASTFunction(
                "toDecimal64",
                std::make_shared<ASTLiteral>(l->value.safeGet<Float64>()),
                std::make_shared<ASTLiteral>(static_cast<UInt64>(fuzz_rand() % 18)));
        else if (decimal == 2)
            child = makeASTFunction(
                "toDecimal128",
                std::make_shared<ASTLiteral>(l->value.safeGet<Float64>()),
                std::make_shared<ASTLiteral>(static_cast<UInt64>(fuzz_rand() % 38)));
        else
            child = makeASTFunction(
                "toDecimal256",
                std::make_shared<ASTLiteral>(l->value.safeGet<Float64>()),
                std::make_shared<ASTLiteral>(static_cast<UInt64>(fuzz_rand() % 76)));
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
        static const constexpr int asterisk_prob = 2000;

        if (auto * /*literal*/ _ = typeid_cast<ASTLiteral *>(child.get()))
        {
            /// Return a '*' literal
            if (fuzz_rand() % asterisk_prob == 0)
                child = std::make_shared<ASTAsterisk>();
            else if (fuzz_rand() % 13 == 0)
                child = fuzzLiteralUnderExpressionList(child);
        }
        else if (fuzz_rand() % asterisk_prob == 0 && dynamic_cast<ASTWithAlias *>(child.get()))
        {
            /// Return a '*' literal
            child = std::make_shared<ASTAsterisk>();
        }
        else
        {
            auto new_child = reverseLiteralFuzzing(child);
            if (new_child)
                child = new_child;
            else
                fuzz(child);
        }
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
                const int index = (fuzz_rand() % 2) == 0 ? (id->name_parts.size() - 1) : (fuzz_rand() % id->name_parts.size());

                clone_parts[index] = alias;
                return std::make_shared<ASTIdentifier>(std::move(clone_parts));
            }
            else if (next_action == 1)
            {
                /// Replace expression with the alias as an identifier
                return std::make_shared<ASTIdentifier>(Strings{alias});
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
                const int nprob = fuzz_rand() % 10;

                /// Pick a random identifier
                auto rand_col1 = colids.begin();
                std::advance(rand_col1, fuzz_rand() % colids.size());
                ASTPtr exp1 = rand_col1->second->clone();

                exp1 = setIdentifierAliasOrNot(exp1);
                if (nprob == 0)
                {
                    next_condition = makeASTFunction(fuzz_rand() % 2 == 0 ? "isNull" : "isNotNull", exp1);
                }
                else
                {
                    /// Pick any other column reference
                    auto rand_col2 = column_like.begin();
                    std::advance(rand_col2, fuzz_rand() % column_like.size());
                    ASTPtr exp2 = rand_col2->second->clone();

                    exp2 = setIdentifierAliasOrNot(exp2);
                    if (fuzz_rand() % 3 == 0)
                    {
                        /// Swap sides
                        auto exp3 = exp1;
                        exp1 = exp2;
                        exp2 = exp3;
                    }
                    /// Run mostly equality conditions
                    /// No isNotDistinctFrom outside join conditions
                    next_condition = makeASTFunction(
                        comparison_comparators[(fuzz_rand() % 10 == 0) ? (fuzz_rand() % (comparison_comparators.size() - 1)) : 0],
                        exp1,
                        exp2);
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

        table_join->locality = locality_values[fuzz_rand() % 2 == 0 ? 0 : (fuzz_rand() % locality_values.size())];
        table_join->kind = kind_values[fuzz_rand() % 2 == 0 ? 0 : (fuzz_rand() % kind_values.size())];
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
        return dynamic_cast<const ASTWithAlias *>(child.get())->tryGetAlias();
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
        auto table_join = std::make_shared<ASTTableJoin>();

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
                otable_exp_alias->setAlias(next_alias);
            }
            else if (dynamic_cast<ASTWithAlias *>(input_table.get()))
            {
                ASTPtr child = input_table->clone();
                table_exp = std::make_shared<ASTTableExpression>();
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
            table_exp = std::make_shared<ASTTableExpression>();
            auto * ntexp = typeid_cast<ASTTableExpression *>(table_exp.get());
            auto new_identifier = std::make_shared<ASTTableIdentifier>(old_alias);
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
            ASTPtr exp1 = std::make_shared<ASTIdentifier>(Strings{next_alias, nidentifier});
            ASTPtr exp2 = rand_col2->second->clone();

            exp2 = setIdentifierAliasOrNot(exp2);
            if (fuzz_rand() % 3 == 0)
            {
                /// Swap sides
                auto exp3 = exp1;
                exp1 = exp2;
                exp2 = exp3;
            }
            /// Run mostly equi-joins
            ASTPtr next_condition = makeASTFunction(
                comparison_comparators[(fuzz_rand() % 10 == 0) ? (fuzz_rand() % comparison_comparators.size()) : 0], exp1, exp2);
            next_condition = tryNegateNextPredicate(next_condition, 30);

            /// Sometimes use multiple conditions
            join_condition
                = join_condition ? makeASTFunction((fuzz_rand() % 10) == 0 ? "or" : "and", join_condition, next_condition) : next_condition;
            join_condition = tryNegateNextPredicate(join_condition, 50);
        }
        chassert(join_condition);
        table_join->children.push_back(join_condition);
        table_join->on_expression = table_join->children.back();

        auto table = std::make_shared<ASTTablesInSelectQueryElement>();
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
        auto array_join = std::make_shared<ASTArrayJoin>();
        array_join->kind = fuzz_rand() % 2 == 0 ? ASTArrayJoin::Kind::Left : ASTArrayJoin::Kind::Inner;
        array_join->children.push_back(arr_join_list);
        array_join->expression_list = array_join->children.back();

        auto table_join = std::make_shared<ASTTablesInSelectQueryElement>();
        table_join->children.push_back(std::move(array_join));
        table_join->array_join = table_join->children.back();
        return table_join;
    }
    return nullptr;
}

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
        fuzz(with_union->list_of_selects);
        /// Fuzzing SELECT query to EXPLAIN query randomly.
        /// And we only fuzzing the root query into an EXPLAIN query, not fuzzing subquery
        if (fuzz_rand() % 20 == 0 && current_ast_depth <= 1)
        {
            auto explain = std::make_shared<ASTExplainQuery>(fuzzExplainKind());

            auto settings_ast = std::make_shared<ASTSetQuery>();
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

        fuzz(table_expr->children);
    }
    else if (auto * expr_list = typeid_cast<ASTExpressionList *>(ast.get()))
    {
        fuzzExpressionList(*expr_list);
    }
    else if (auto * order_by_element = typeid_cast<ASTOrderByElement *>(ast.get()))
    {
        fuzzOrderByElement(order_by_element);
    }
    else if (auto * fn = typeid_cast<ASTFunction *>(ast.get()))
    {
        static const std::unordered_set<String> & cast_functions = {"_CAST", "CAST", "accurateCast", "accurateCastOrNull"};

        fuzzColumnLikeExpressionList(fn->arguments.get());
        fuzzColumnLikeExpressionList(fn->parameters.get());

        /// fuzzColumnLikeExpressionList may remove arguments
        const size_t nargs = fn->arguments ? fn->arguments->children.size() : 0;

        if (nargs == 2 && fuzz_rand() % 30 == 0 && cast_functions.contains(fn->name))
        {
            /// Fuzz casts
            const auto old_type = DataTypeFactory::instance().tryGet(fn->arguments->children[1]);
            const auto new_type = old_type ? fuzzDataType(old_type) : getRandomType();

            fn->arguments->children[1] = std::make_shared<ASTLiteral>(new_type->getName());
        }
        else if (AggregateUtils::isAggregateFunction(*fn))
        {
            if (nargs > 0)
            {
                if (nargs < 3 && fuzz_rand() % 30 == 0)
                {
                    /// Replace aggregate function
                    static const std::map<size_t, Strings> & aggrs
                        = {{1,
                            {"any",
                             "anyHeavy",
                             "anyLast",
                             "avg",
                             "count",
                             "deltaSum",
                             "entropy",
                             "first_value",
                             "kurtPop",
                             "kurtSamp",
                             "last_value",
                             "max",
                             "median",
                             "min",
                             "rankCorr",
                             "skewPop",
                             "skewSamp",
                             "stddevPop",
                             "stddevPopStable",
                             "stddevSamp",
                             "stddevSampStable",
                             "sum",
                             "sumCount",
                             "sumKahan",
                             "uniq",
                             "varPop",
                             "varSamp"}},
                           {2,
                            {"argMax",
                             "argMin",
                             "avgWeighted",
                             "boundingRatio",
                             "corr",
                             "covarPop",
                             "covarPopStable",
                             "deltaSumTimestamp",
                             "maxIntersections",
                             "maxIntersectionsPosition",
                             "uniq"}}};
                    const Strings & commonAggrs = aggrs.at(nargs);

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
                    static const String & distinctSuffix = "Distinct";

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
            fuzzNullsAction(fn->nulls_action);
        }
        else if (fuzz_rand() % 30 == 0)
        {
            /// Swap function name
            static const std::vector<std::unordered_set<String>> & swapFuncs
                = {{"ilike", "like", "match", "notILike", "notLike"},
                   {"globalIn", "globalNotIn", "in", "notIn"},
                   {"equals", "isNotDistinctFrom"},
                   {"assumeNotNull", "isNotNull", "isNull", "isNullable", "isZeroOrNull", "toNullable"},
                   {"clamp", "coalesce", "greatest", "least"},
                   {"greater", "greaterOrEquals", "less", "lessOrEquals", "notEquals"},
                   {"concat", "divide", "intDiv", "minus", "modulo", "multiply", "plus"},
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
                   {"toUnixTimestamp64Micro", "toUnixTimestamp64Milli", "toUnixTimestamp64Nano", "toUnixTimestamp64Second"},
                   {"addDays",
                    "addHours",
                    "addInterval",
                    "addMicroseconds",
                    "addMilliseconds",
                    "addMinutes",
                    "addMonths",
                    "addNanoseconds",
                    "addQuarters",
                    "addSeconds",
                    "addTupleOfIntervals",
                    "addWeeks",
                    "addYears",
                    "subtractDays",
                    "subtractHours",
                    "subtractInterval",
                    "subtractMicroseconds",
                    "subtractMilliseconds",
                    "subtractMinutes",
                    "subtractMonths",
                    "subtractNanoseconds",
                    "subtractQuarters",
                    "subtractSeconds",
                    "subtractTupleOfIntervals",
                    "subtractWeeks",
                    "subtractYears"}};

            for (const auto & entry : swapFuncs)
            {
                if (entry.contains(fn->name))
                {
                    fn->name = pickRandomly(fuzz_rand, entry);
                    break;
                }
            }
        }

        if (fn->is_window_function && fn->window_definition)
        {
            auto & def = fn->window_definition->as<ASTWindowDefinition &>();
            fuzzColumnLikeExpressionList(def.partition_by.get());
            fuzzOrderByList(def.order_by.get(), 0);
            fuzzWindowFrame(def);
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
                    select->having()->children.clear();

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
                select->where()->children.clear();
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
                select->prewhere()->children.clear();
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
                select->qualify()->children.clear();
                addOrReplacePredicate(select, ASTSelectQuery::Expression::QUALIFY);
            }
        }
        else if (fuzz_rand() % 50 == 0)
        {
            addOrReplacePredicate(select, ASTSelectQuery::Expression::QUALIFY);
        }

        fuzzOrderByList(select->orderBy().get(), select->select()->children.size());
        if (select->orderBy().get() && fuzz_rand() % 50 == 0)
        {
            select->order_by_all = !select->order_by_all;
        }
        if (select->limitLength() && fuzz_rand() % 50 == 0)
        {
            select->limit_with_ties = !select->limit_with_ties;
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
        fuzzCreateQuery(*create_query);
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

    if (out_stream)
    {
        *out_stream << std::endl;
        *out_stream << ast->formatWithSecretsOneLine() << std::endl << std::endl;
    }
}

}
