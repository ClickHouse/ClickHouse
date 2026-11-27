#include <Storages/MergeTree/MergeTreeLowCardinalityDictionariesFilter.h>

#include <Compression/CompressedReadBufferFromFile.h>
#include <Core/Settings.h>
#include <DataTypes/Serializations/SerializationLowCardinality.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/Regexps.h>
#include <Functions/indexHint.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeIndexSet.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/getEffectiveRowPolicyFilter.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>


namespace ProfileEvents
{
    extern const Event LowCardinalityDictionariesCheckedParts;
    extern const Event LowCardinalityDictionariesPrunedParts;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool use_low_cardinality_dictionaries_for_part_pruning;
    extern const SettingsUInt64 max_low_cardinality_dictionaries_bytes_for_part_pruning;
    extern const SettingsUInt64 min_marks_per_part_to_load_statistics;
}

namespace
{

/// Whether every function of `node` that depends on the input `column_name` cannot throw on any value of its arguments.
/// Pruning by dictionaries evaluates the filter on all values of a part, including the values of the rows that the query
/// skips by other indexes or does not read at all, so a function that can throw on some values (e.g. a conversion, or
/// `CAST` of `NULL` to a non-Nullable type) would make a query fail that succeeds without the pruning.
/// Also, the filter must not contain functions whose result depends on the block or the row rather than on the values
/// of the arguments (e.g. `blockSize`, `rowNumberInBlock`, `rand`), even if they do not depend on the column, because
/// they are evaluated on the block of dictionary values, which differs from the blocks of the rows.
/// A function that does not depend on any column, such as `throwIf(1)`, is evaluated too, and the `set` index evaluates
/// every argument of `and` and `or`, even where the row filter would short-circuit, so such a function must not throw either.
/// A subtree that depends on another column, or that is folded to a constant, is not evaluated by the `set` index.
class SafeFunctionsChecker
{
public:
    explicit SafeFunctionsChecker(const String & column_name_) : column_name(column_name_) {}

    bool isSafe(const ActionsDAG::Node & node) { return check(node).safe; }

private:
    struct Result
    {
        /// Depends on the column.
        bool depends = false;
        /// Depends on another column, so the `set` index replaces the subtree by an unknown value.
        bool depends_on_other_columns = false;
        bool safe = true;
    };

    const String & column_name;
    std::unordered_map<const ActionsDAG::Node *, Result> visited;

    static const ActionsDAG::NodeRawConstPtrs & getArguments(const ActionsDAG::Node & node)
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base || node.function_base->getName() != "indexHint")
            return node.children;

        /// The arguments of `indexHint` are stored inside of `FunctionIndexHint`, and the `set` index evaluates them.
        const auto & adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor &>(*node.function_base);
        const auto & index_hint = typeid_cast<const FunctionIndexHint &>(*adaptor.getFunction());
        return index_hint.getActions().getOutputs();
    }

    Result check(const ActionsDAG::Node & node)
    {
        if (auto it = visited.find(&node); it != visited.end())
            return it->second;

        Result result;

        /// The `set` index uses a constant as is, without evaluating the subtree it was folded from.
        if (node.column)
        {
            visited[&node] = result;
            return result;
        }

        const auto & arguments = getArguments(node);
        std::vector<Result> argument_results;
        argument_results.reserve(arguments.size());
        for (const auto * argument : arguments)
        {
            auto argument_result = check(*argument);
            result.depends = result.depends || argument_result.depends;
            result.depends_on_other_columns = result.depends_on_other_columns || argument_result.depends_on_other_columns;
            result.safe = result.safe && argument_result.safe;
            argument_results.push_back(argument_result);
        }

        if (node.type == ActionsDAG::ActionType::INPUT)
        {
            result.depends = node.result_name == column_name;
            result.depends_on_other_columns = !result.depends;
        }
        else if (node.type == ActionsDAG::ActionType::ARRAY_JOIN)
        {
            result.safe = result.safe && !result.depends;
        }
        else if (node.type == ActionsDAG::ActionType::FUNCTION)
        {
            if (!node.function_base || node.function_base->isStateful() || !node.function_base->isDeterministicInScopeOfQuery())
            {
                result.safe = false;
            }
            else if (result.depends || !result.depends_on_other_columns)
            {
                const auto & name = node.function_base->getName();
                if (functions_with_pattern.contains(name))
                    result.safe = result.safe && isPatternValid(node);
                else
                    result.safe = result.safe && safe_functions.contains(name) && !canThrowOnArguments(node);
            }
        }

        visited[&node] = result;
        return result;
    }

    static DataTypesWithConstInfo getArgumentTypes(const ActionsDAG::Node & node)
    {
        DataTypesWithConstInfo arguments;
        arguments.reserve(node.children.size());
        for (const auto * child : node.children)
            arguments.push_back({child->result_type, child->column && isColumnConst(*child->column),
                child->column ? typeid_cast<const ColumnConst *>(child->column.get()) : nullptr});
        return arguments;
    }

    /// The name of a function is not enough: whether it can throw depends on the types of its arguments.
    /// For example, a comparison of `UInt64` with `Decimal` converts the number and throws `DECIMAL_OVERFLOW`
    /// if it does not fit, and a comparison of a date with a string parses the string.
    /// The comparisons always describe it. The other functions of the list are trusted by name
    /// unless they say that they can throw on these arguments.
    static bool canThrowOnArguments(const ActionsDAG::Node & node)
    {
        const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(node.function_base.get());
        if (!adaptor)
            return comparison_functions.contains(node.function_base->getName());

        const auto can_throw = adaptor->getFunction()->canThrowIfKnown(getArgumentTypes(node));
        if (comparison_functions.contains(node.function_base->getName()))
            return can_throw.value_or(true);
        return can_throw.value_or(false);
    }

    /// The pattern is compiled and can be invalid, so it must be a constant that is known to compile.
    /// A pattern that is not folded to a constant, such as `materialize('[')`, is compiled only for the rows the query
    /// evaluates the function on, and a constant one is not compiled at all inside of `indexHint`.
    static bool isPatternValid(const ActionsDAG::Node & node)
    {
        if (node.children.size() != 2)
            return false;

        const auto & pattern_column = node.children[1]->column;
        const ColumnConst * pattern = pattern_column ? checkAndGetColumnConstStringOrFixedString(pattern_column.get()) : nullptr;
        if (!pattern)
            return false;

        /// `like` and similar describe it themselves.
        if (node.function_base->getName() != "match")
        {
            auto arguments = getArgumentTypes(node);
            arguments[1].column = pattern;
            return !node.function_base->canThrow(arguments);
        }

        try
        {
            Regexps::createRegexp</*like=*/ false, /*similar_to=*/ false, /*no_capture=*/ true, /*case_insensitive=*/ false>(
                pattern->getValue<String>());
        }
        catch (const Exception &)
        {
            return false;
        }
        return true;
    }

    static inline const std::unordered_set<std::string_view> safe_functions = {
        "equals", "notEquals", "less", "greater", "lessOrEquals", "greaterOrEquals",
        "in", "notIn", "globalIn", "globalNotIn", "nullIn", "notNullIn", "globalNullIn", "globalNotNullIn",
        "startsWith", "endsWith", "startsWithUTF8", "endsWithUTF8",
        "position", "positionCaseInsensitive", "positionUTF8", "positionCaseInsensitiveUTF8",
        "empty", "notEmpty", "length", "lengthUTF8",
        "lower", "upper", "lowerUTF8", "upperUTF8",
        "isNull", "isNotNull", "coalesce", "ifNull", "assumeNotNull", "toLowCardinality", "materialize", "identity",
        "and", "or", "not", "xor", "indexHint",
    };

    /// Safe only for the argument types that are compared without a conversion, see `comparisonCanThrow`.
    static inline const std::unordered_set<std::string_view> comparison_functions = {
        "equals", "notEquals", "less", "greater", "lessOrEquals", "greaterOrEquals",
    };

    /// Safe if the pattern is a valid constant.
    static inline const std::unordered_set<std::string_view> functions_with_pattern = {
        "like", "notLike", "ilike", "notILike", "match",
    };
};

}

/// A skip index of type `set` on a top-level `LowCardinality` column used by the filter.
struct MergeTreeLowCardinalityDictionariesFilter::ColumnCondition
{
    NameAndTypePair column;
    std::shared_ptr<const SerializationLowCardinality> serialization;
    /// `IMergeTreeIndex` refers to its description, which is not in the metadata.
    std::shared_ptr<const IndexDescription> index_description;
    MergeTreeIndexPtr index;
    MergeTreeIndexConditionPtr condition;
};

MergeTreeLowCardinalityDictionariesFilterPtr MergeTreeLowCardinalityDictionariesFilter::create(
    const MergeTreeData & data,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot,
    const ContextPtr & context)
{
    /// The dictionaries describe the values as they are stored in the part, so they are not used in the same cases
    /// as statistics, see `filterPartsByStatistics`: with `FINAL`, with on-the-fly mutations or patch parts, with
    /// masking policies and row policies. Pending alter and metadata mutations are checked per part in `canMatch`.
    if (!context->getSettingsRef()[Setting::use_low_cardinality_dictionaries_for_part_pruning]
        || !query_info.filter_actions_dag
        || query_info.isFinal()
        || (mutations_snapshot && (mutations_snapshot->hasDataMutations() || mutations_snapshot->hasPatchParts()))
        || data.hasEnabledMaskingPolicies(context)
        || query_info.row_level_filter
        || getEffectiveRowPolicyFilter(data, context))
    {
        return nullptr;
    }

    auto filter = std::make_shared<MergeTreeLowCardinalityDictionariesFilter>(metadata_snapshot, query_info, mutations_snapshot, context);
    if (filter->conditions.empty())
        return nullptr;
    return filter;
}

MergeTreeLowCardinalityDictionariesFilter::MergeTreeLowCardinalityDictionariesFilter(
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot_,
    const ContextPtr & context_)
    : mutations_snapshot(mutations_snapshot_)
    , context(context_)
    , max_dictionaries_bytes(context->getSettingsRef()[Setting::max_low_cardinality_dictionaries_bytes_for_part_pruning])
    , min_marks_per_part(context->getSettingsRef()[Setting::min_marks_per_part_to_load_statistics])
    , log(getLogger("MergeTreeLowCardinalityDictionariesFilter"))
{
    const auto * predicate = query_info.filter_actions_dag->getOutputs().front();
    const auto & columns = metadata_snapshot->getColumns();

    NameSet used_columns;
    for (const auto * input : query_info.filter_actions_dag->getInputs())
    {
        if (!used_columns.insert(input->result_name).second)
            continue;

        auto column = columns.tryGetPhysical(input->result_name);
        if (!column || !column->type->lowCardinality())
            continue;

        auto serialization = std::dynamic_pointer_cast<const SerializationLowCardinality>(column->type->getDefaultSerialization());
        if (!serialization)
            continue;

        if (!SafeFunctionsChecker(column->name).isSafe(*predicate))
            continue;

        auto index_description = std::make_shared<const IndexDescription>(createImplicitSetIndexDescription(column->name, columns, context));
        auto index = std::make_shared<MergeTreeIndexSet>(metadata_snapshot, *index_description, /*max_rows=*/ 0);
        auto condition = index->createIndexCondition(predicate, context);
        if (!condition || condition->alwaysUnknownOrTrue())
            continue;

        conditions.push_back({*column, std::move(serialization), std::move(index_description), std::move(index), std::move(condition)});
    }
}

MergeTreeLowCardinalityDictionariesFilter::~MergeTreeLowCardinalityDictionariesFilter() = default;

Names MergeTreeLowCardinalityDictionariesFilter::getColumnNames() const
{
    Names names;
    names.reserve(conditions.size());
    for (const auto & condition : conditions)
        names.push_back(condition.column.name);
    return names;
}

bool MergeTreeLowCardinalityDictionariesFilter::canMatch(const MergeTreeData::DataPartPtr & part) const
{
    if (!isWidePart(part) || MergeTreeDataSelectExecutor::partHasPendingAlterOrMetadataMutations(part, mutations_snapshot))
        return true;

    /// The dictionaries are read synchronously and without prefetching, so on a remote disk
    /// the round trips can cost more than the reads that the pruning saves.
    if (part->getDataPartStorage().isStoredOnRemoteDisk())
        return true;

    ISerialization::SubstreamPath dictionary_keys_path;
    dictionary_keys_path.emplace_back(ISerialization::Substream::DictionaryKeys);

    bool checked = false;
    for (const auto & condition : conditions)
    {
        /// The column is converted on read if the part stores it with another type, and absent in old parts.
        auto part_column = part->tryGetColumn(condition.column.name);
        if (!part_column || !part_column->type->equals(*condition.column.type))
            continue;

        if (part->hasIncompleteLowCardinalityDictionaries(part_column->name))
            continue;

        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
            *part_column, dictionary_keys_path, ".bin", part->checksums, part->storage.getSettings());
        if (!stream_name)
            continue;

        const auto file_name = *stream_name + ".bin";
        const size_t file_size = part->checksums.files.at(file_name).file_size;
        if (file_size > max_dictionaries_bytes)
            continue;

        auto file = part->getDataPartStorage().readFile(file_name, context->getReadSettings().adjustBufferSize(file_size), file_size);
        CompressedReadBufferFromFile keys_stream(std::move(file), /* allow_different_codecs */ true);
        auto dictionaries = condition.serialization->deserializeCompleteDictionaries(keys_stream);
        if (!dictionaries)
        {
            part->setIncompleteLowCardinalityDictionaries(part_column->name);
            continue;
        }

        checked = true;

        /// Each dictionary is a granule of the `set` index, and the part can match if any of them can.
        /// The condition is evaluated on every value anyway, so the range of values is not computed
        /// to check the condition on it first: it would materialize every value.
        const auto & index_description = condition.index->index;
        const bool can_match_by_column = std::ranges::any_of(*dictionaries, [&](const ColumnPtr & values)
        {
            auto granule = std::make_shared<MergeTreeIndexGranuleSet>(index_description.name, index_description.sample_block, /*max_rows=*/ 0);
            granule->block = index_description.sample_block.cloneWithColumns(Columns{values});
            granule->set_hyperrectangle.assign(1, Range::createWholeUniverse());
            return condition.condition->mayBeTrueOnGranule(granule, {});
        });

        if (!dictionaries->empty() && !can_match_by_column)
        {
            LOG_TRACE(log, "Part {} pruned by the dictionaries of column {}", part->name, condition.column.name);
            ProfileEvents::increment(ProfileEvents::LowCardinalityDictionariesCheckedParts);
            ProfileEvents::increment(ProfileEvents::LowCardinalityDictionariesPrunedParts);
            return false;
        }
    }

    if (checked)
        ProfileEvents::increment(ProfileEvents::LowCardinalityDictionariesCheckedParts);
    return true;
}

}
