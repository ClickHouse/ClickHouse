#include "QueryFuzzer.h"
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ASTDropQuery.h>

#include <unordered_set>

#include <Core/Types.h>
#include <IO/Operators.h>
#include <IO/UseSSL.h>
#include <IO/WriteBufferFromOStream.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <pcg_random.hpp>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
}

Field QueryFuzzer::getRandomField(int type)
{
    static constexpr Int64 bad_int64_values[]
        = {-2, -1, 0, 1, 2, 3, 7, 10, 100, 255, 256, 257, 1023, 1024,
           1025, 65535, 65536, 65537, 1024 * 1024 - 1, 1024 * 1024,
           1024 * 1024 + 1, INT_MIN - 1ll, INT_MIN, INT_MIN + 1,
           INT_MAX - 1, INT_MAX, INT_MAX + 1ll, INT64_MIN, INT64_MIN + 1,
           INT64_MAX - 1, INT64_MAX};
    switch (type)
    {
    case 0:
    {
        return bad_int64_values[fuzz_rand() % std::size(bad_int64_values)];
    }
    case 1:
    {
        static constexpr double values[]
                = {NAN, INFINITY, -INFINITY, 0., -0., 0.0001, 0.5, 0.9999,
                   1., 1.0001, 2., 10.0001, 100.0001, 1000.0001, 1e10, 1e20,
                  FLT_MIN, FLT_MIN + FLT_EPSILON, FLT_MAX, FLT_MAX + FLT_EPSILON}; return values[fuzz_rand() % std::size(values)];
    }
    case 2:
    {
        static constexpr UInt64 scales[] = {0, 1, 2, 10};
        return DecimalField<Decimal64>(
            bad_int64_values[fuzz_rand() % std::size(bad_int64_values)],
            static_cast<UInt32>(scales[fuzz_rand() % std::size(scales)])
        );
    }
    default:
        assert(false);
        return Null{};
    }
}

Field QueryFuzzer::fuzzField(Field field)
{
    const auto type = field.getType();

    int type_index = -1;

    if (type == Field::Types::Int64
        || type == Field::Types::UInt64)
    {
        type_index = 0;
    }
    else if (type == Field::Types::Float64)
    {
        type_index = 1;
    }
    else if (type == Field::Types::Decimal32
        || type == Field::Types::Decimal64
        || type == Field::Types::Decimal128
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
        UInt64 action = fuzz_rand() % 10;
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
        default:
            // Do nothing
            break;
        }
    }
    else if (type == Field::Types::Array)
    {
        auto & arr = field.safeGet<Array>();

        if (fuzz_rand() % 5 == 0 && !arr.empty())
        {
            size_t pos = fuzz_rand() % arr.size();
            arr.erase(arr.begin() + pos);
            if (debug_stream)
                *debug_stream << "erased\n";
        }

        if (fuzz_rand() % 5 == 0)
        {
            if (!arr.empty())
            {
                size_t pos = fuzz_rand() % arr.size();
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
            size_t pos = fuzz_rand() % arr.size();
            arr.erase(arr.begin() + pos);

            if (debug_stream)
                *debug_stream << "erased\n";
        }

        if (fuzz_rand() % 5 == 0)
        {
            if (!arr.empty())
            {
                size_t pos = fuzz_rand() % arr.size();
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
    new_ast->setAlias("");

    return new_ast;
}

ASTPtr QueryFuzzer::getRandomExpressionList()
{
    if (column_like.empty())
    {
        return nullptr;
    }

    ASTPtr new_ast = std::make_shared<ASTExpressionList>();
    for (size_t i = 0; i < fuzz_rand() % 5 + 1; ++i)
    {
        new_ast->children.push_back(getRandomColumnLike());
    }
    return new_ast;
}

void QueryFuzzer::replaceWithColumnLike(ASTPtr & ast)
{
    if (column_like.empty())
    {
        return;
    }

    std::string old_alias = ast->tryGetAlias();
    ast = getRandomColumnLike();
    ast->setAlias(old_alias);
}

void QueryFuzzer::replaceWithTableLike(ASTPtr & ast)
{
    if (table_like.empty())
    {
        return;
    }

    ASTPtr new_ast = table_like[fuzz_rand() % table_like.size()].second->clone();

    std::string old_alias = ast->tryGetAlias();
    new_ast->setAlias(old_alias);

    ast = new_ast;
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

void QueryFuzzer::fuzzOrderByList(IAST * ast)
{
    if (!ast)
    {
        return;
    }

    auto * list = assert_cast<ASTExpressionList *>(ast);

    // Remove element
    if (fuzz_rand() % 50 == 0 && list->children.size() > 1)
    {
        // Don't remove last element -- this leads to questionable
        // constructs such as empty select.
        list->children.erase(list->children.begin()
                             + fuzz_rand() % list->children.size());
    }

    // Add element
    if (fuzz_rand() % 50 == 0)
    {
        auto * pos = list->children.empty() ? list->children.begin() : list->children.begin() + fuzz_rand() % list->children.size();
        auto col = getRandomColumnLike();
        if (col)
        {
            auto elem = std::make_shared<ASTOrderByElement>();
            elem->children.push_back(col);
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

    auto * impl = assert_cast<ASTExpressionList *>(ast);

    // Remove element
    if (fuzz_rand() % 50 == 0 && impl->children.size() > 1)
    {
        // Don't remove last element -- this leads to questionable
        // constructs such as empty select.
        impl->children.erase(impl->children.begin()
                             + fuzz_rand() % impl->children.size());
    }

    // Add element
    if (fuzz_rand() % 50 == 0)
    {
        auto * pos = impl->children.empty() ? impl->children.begin() : impl->children.begin() + fuzz_rand() % impl->children.size();
        auto col = getRandomColumnLike();
        if (col)
            impl->children.insert(pos, col);
        else
            if (debug_stream)
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
            case 0:
            {
                action = NullsAction::EMPTY;
                break;
            }
            case 1:
            {
                action = NullsAction::RESPECT_NULLS;
                break;
            }
            default:
            {
                action = NullsAction::IGNORE_NULLS;
                break;
            }
        }
    }
}

void QueryFuzzer::fuzzWindowFrame(ASTWindowDefinition & def)
{
    switch (fuzz_rand() % 40)
    {
        case 0:
        {
            const auto r = fuzz_rand() % 3;
            def.frame_type = r == 0 ? WindowFrame::FrameType::ROWS
                : r == 1 ? WindowFrame::FrameType::RANGE
                    : WindowFrame::FrameType::GROUPS;
            break;
        }
        case 1:
        {
            const auto r = fuzz_rand() % 3;
            def.frame_begin_type = r == 0 ? WindowFrame::BoundaryType::Unbounded
                : r == 1 ? WindowFrame::BoundaryType::Current
                    : WindowFrame::BoundaryType::Offset;

            if (def.frame_begin_type == WindowFrame::BoundaryType::Offset)
            {
                // The offsets are fuzzed normally through 'children'.
                def.frame_begin_offset
                    = std::make_shared<ASTLiteral>(getRandomField(0));
            }
            else
            {
                def.frame_begin_offset = nullptr;
            }
            break;
        }
        case 2:
        {
            const auto r = fuzz_rand() % 3;
            def.frame_end_type = r == 0 ? WindowFrame::BoundaryType::Unbounded
                : r == 1 ? WindowFrame::BoundaryType::Current
                    : WindowFrame::BoundaryType::Offset;

            if (def.frame_end_type == WindowFrame::BoundaryType::Offset)
            {
                def.frame_end_offset
                    = std::make_shared<ASTLiteral>(getRandomField(0));
            }
            else
            {
                def.frame_end_offset = nullptr;
            }
            break;
        }
        case 5:
        {
            def.frame_begin_preceding = fuzz_rand() % 2;
            break;
        }
        case 6:
        {
            def.frame_end_preceding = fuzz_rand() % 2;
            break;
        }
        default:
            break;
    }

    if (def.frame_type == WindowFrame::FrameType::RANGE
        && def.frame_begin_type == WindowFrame::BoundaryType::Unbounded
        && def.frame_begin_preceding
        && def.frame_end_type == WindowFrame::BoundaryType::Current)
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
        create.columns_list->updateTreeHash(sip_hash, /*ignore_aliases=*/ true);
    if (create.storage)
        create.storage->updateTreeHash(sip_hash, /*ignore_aliases=*/ true);

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
        column.type = parseQuery(parser, data_type->getName(),
            DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
}

DataTypePtr QueryFuzzer::fuzzDataType(DataTypePtr type)
{
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

        return type_tuple->haveExplicitNames()
            ? std::make_shared<DataTypeTuple>(elements, type_tuple->getElementNames())
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
    auto type_id = static_cast<TypeIndex>(fuzz_rand() % static_cast<size_t>(TypeIndex::Tuple) + 1);

    if (type_id == TypeIndex::Tuple)
    {
        size_t tuple_size = fuzz_rand() % 6 + 1;
        DataTypes elements;
        for (size_t i = 0; i < tuple_size; ++i)
            elements.push_back(getRandomType());
        return std::make_shared<DataTypeTuple>(elements);
    }

    if (type_id == TypeIndex::Array)
        return std::make_shared<DataTypeArray>(getRandomType());

/// NOLINTBEGIN(bugprone-macro-parentheses)
#define DISPATCH(DECIMAL) \
    if (type_id == TypeIndex::DECIMAL) \
        return std::make_shared<DataTypeDecimal<DECIMAL>>( \
            DataTypeDecimal<DECIMAL>::maxPrecision(), \
            (fuzz_rand() % DataTypeDecimal<DECIMAL>::maxPrecision()) + 1);

    DISPATCH(Decimal32)
    DISPATCH(Decimal64)
    DISPATCH(Decimal128)
    DISPATCH(Decimal256)
#undef DISPATCH
/// NOLINTEND(bugprone-macro-parentheses)

    if (type_id == TypeIndex::FixedString)
        return std::make_shared<DataTypeFixedString>(fuzz_rand() % 20);

    if (type_id == TypeIndex::Enum8)
        return std::make_shared<DataTypeUInt8>();

    if (type_id == TypeIndex::Enum16)
        return std::make_shared<DataTypeUInt16>();

    return DataTypeFactory::instance().get(String(magic_enum::enum_name(type_id)));
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
           {ASTExplainQuery::ExplainKind::AnalyzedSyntax, {}},
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

static ASTPtr tryParseInsertQuery(const String & full_query)
{
    const char * pos = full_query.data();
    const char * end = full_query.data() + full_query.size();

    ParserInsertQuery parser(end, false);
    String message;

    return tryParseQuery(parser, pos, end, message, false, "", false,
        DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS, true);
}

ASTs QueryFuzzer::getInsertQueriesForFuzzedTables(const String & full_query)
{
    auto parsed_query = tryParseInsertQuery(full_query);
    if (!parsed_query)
        return {};

    const auto & insert = *parsed_query->as<ASTInsertQuery>();
    if (!insert.table)
        return {};

    auto table_name = insert.getTable();
    auto it = original_table_name_to_fuzzed.find(table_name);
    if (it == original_table_name_to_fuzzed.end())
        return {};

    ASTs queries;
    for (const auto & fuzzed_name : it->second)
    {
        /// Parse query from scratch for each table instead of clone,
        /// to store proper pointers to inlined data,
        /// which are not copied during clone.
        auto & query = queries.emplace_back(tryParseInsertQuery(full_query));
        query->as<ASTInsertQuery>()->setTable(fuzzed_name);
    }

    return queries;
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
    auto * l = child->as<ASTLiteral>();
    chassert(l);
    auto type = l->value.getType();
    if (type == Field::Types::Which::String && fuzz_rand() % 7 == 0)
    {
        String value = l->value.safeGet<String>();
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
        int decimal = fuzz_rand() % 4;
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
        const std::unordered_set<String> can_be_reverted{
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
    for (auto & child : expr_list.children)
    {
        if (auto * /*literal*/ _ = typeid_cast<ASTLiteral *>(child.get()))
        {
            if (fuzz_rand() % 13 == 0)
                child = fuzzLiteralUnderExpressionList(child);
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

    explicit ScopedIncrement(size_t & counter_) : counter(counter_) { ++counter; }
    ~ScopedIncrement() { --counter; }
};

void QueryFuzzer::fuzz(ASTPtr & ast)
{
    if (!ast)
        return;

    // Check for exceeding max depth.
    ScopedIncrement depth_increment(current_ast_depth);
    if (current_ast_depth > 500)
    {
        // The AST is too deep (see the comment for current_ast_depth). Throw
        // an exception to fail fast and not use this query as an etalon, or we'll
        // end up in a very slow and useless loop. It also makes sense to set it
        // lower than the default max parse depth on the server (1000), so that
        // we don't get the useless error about parse depth from the server either.
        throw Exception(ErrorCodes::TOO_DEEP_RECURSION,
            "AST depth exceeded while fuzzing ({})", current_ast_depth);
    }

    // Check for loops.
    auto [_, inserted] = debug_visited_nodes.insert(ast.get());
    if (!inserted)
    {
        fmt::print(stderr, "The AST node '{}' was already visited before."
            " Depth {}, {} visited nodes, current top AST:\n{}\n",
            static_cast<void *>(ast.get()), current_ast_depth,
            debug_visited_nodes.size(), (*debug_top_ast)->dumpTree());
        assert(false);
    }

    // The fuzzing.
    if (auto * with_union = typeid_cast<ASTSelectWithUnionQuery *>(ast.get()))
    {
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
        fuzzColumnLikeExpressionList(fn->arguments.get());
        fuzzColumnLikeExpressionList(fn->parameters.get());

        if (AggregateUtils::isAggregateFunction(*fn))
            fuzzNullsAction(fn->nulls_action);

        if (fn->is_window_function && fn->window_definition)
        {
            auto & def = fn->window_definition->as<ASTWindowDefinition &>();
            fuzzColumnLikeExpressionList(def.partition_by.get());
            fuzzOrderByList(def.order_by.get());
            fuzzWindowFrame(def);
        }

        fuzz(fn->children);
    }
    else if (auto * select = typeid_cast<ASTSelectQuery *>(ast.get()))
    {
        fuzzColumnLikeExpressionList(select->select().get());

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
        }
        else if (fuzz_rand() % 50 == 0)
        {
            select->setExpression(ASTSelectQuery::Expression::GROUP_BY, getRandomExpressionList());
        }

        if (select->where().get())
        {
            if (fuzz_rand() % 50 == 0)
            {
                select->where()->children.clear();
                select->setExpression(ASTSelectQuery::Expression::WHERE, {});
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
            select->setExpression(ASTSelectQuery::Expression::WHERE, getRandomColumnLike());
        }

        if (select->prewhere().get())
        {
            if (fuzz_rand() % 50 == 0)
            {
                select->prewhere()->children.clear();
                select->setExpression(ASTSelectQuery::Expression::PREWHERE, {});
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
            select->setExpression(ASTSelectQuery::Expression::PREWHERE, getRandomColumnLike());
        }

        fuzzOrderByList(select->orderBy().get());

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
        const auto ast_del = *iter;
        table_like.erase(iter);
        table_like_map.erase(ast_del.first);
    }

    const auto name = ast->formatForErrorMessage();
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
        const auto ast_del = *iter;
        column_like.erase(iter);
        column_like_map.erase(ast_del.first);
    }

    const auto name = ast->formatForErrorMessage();
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
    else if (typeid_cast<ASTFunction *>(ast.get()))
    {
        addColumnLike(ast);
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
    debug_visited_nodes.clear();
    debug_top_ast = &ast;

    collectFuzzInfoMain(ast);
    fuzz(ast);

    if (out_stream)
    {
        *out_stream << std::endl;

        WriteBufferFromOStream ast_buf(*out_stream, 4096);
        formatAST(*ast, ast_buf, false /*highlight*/);
        ast_buf.finalize();
        *out_stream << std::endl << std::endl;
    }
}

}
