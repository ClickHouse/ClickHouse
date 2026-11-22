#include <Storages/MergeTree/NestedDefaultsReconciliation.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/MaterializedColumnDependencies.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

namespace
{

/// Resolve a source-column identifier used inside a `DEFAULT` expression to the name of the
/// storage column it reads through. Physical columns and their subcolumns resolve directly; a
/// subcolumn read through an `ALIAS` column (whose subcolumns are not registered) is resolved by
/// finding the longest existing column name that is a dotted prefix of the identifier. Iterating
/// over the actual column names (rather than peeling one dotted segment) correctly handles both
/// nested subcolumns and legal quoted column names that themselves contain dots (e.g.
/// `` `x.y` ALIAS m `` referenced as `` `x.y`.keys ``). Returns an empty string if nothing matches.
String resolveDefaultDependencyToStorage(const String & required_name, const ColumnsDescription & columns_desc)
{
    /// `withSubcolumns` resolves subcolumns of *physical* columns (e.g. `m.keys` -> `m`).
    if (auto resolved = columns_desc.tryGetColumn(
            GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), required_name))
        return resolved->getNameInStorage();

    /// Subcolumns of `ALIAS` columns are not registered (`ColumnsDescription::addSubcolumns` skips
    /// `ColumnDefaultKind::Alias`), so an identifier like `m_alias.keys` with `m_alias ALIAS m` does
    /// not resolve above. Find the longest existing column name that is a dotted prefix of the
    /// identifier so the alias expansion below still reaches the physical dependency, even for alias
    /// names that contain dots themselves.
    String storage_name;
    for (const auto & column : columns_desc.getAll())
    {
        if (required_name.size() > column.name.size()
            && required_name.starts_with(column.name)
            && required_name[column.name.size()] == '.'
            && column.name.size() > storage_name.size())
            storage_name = column.name;
    }
    return storage_name;
}

/// Whether the expression contains no function known to be non-deterministic (e.g. `rand64`, `now`).
bool hasOnlyDeterministicFunctions(const ASTPtr & expression, const ContextPtr & context)
{
    if (!expression)
        return true;

    if (const auto * function = expression->as<ASTFunction>())
    {
        /// Only the name: resolving it does not touch the dictionary for `dictGet` and the like.
        if (auto resolver = FunctionFactory::instance().tryGet(function->name, context); resolver && !resolver->isDeterministic())
            return false;
    }

    for (const auto & child : expression->children)
        if (!hasOnlyDeterministicFunctions(child, context))
            return false;

    return true;
}

}

NameSet collectDefaultStorageDependencies(
    const ASTPtr & default_expression,
    const ColumnsDescription & columns_desc,
    const NameSet & columns_to_follow)
{
    NameSet result;
    NameSet visited_columns;

    std::vector<ASTPtr> to_visit;
    to_visit.push_back(default_expression);

    while (!to_visit.empty())
    {
        /// Clone defensively so the shared column-description AST is never touched, mirroring
        /// how `evaluateMissingDefaults` analyses a cloned copy of the default expression.
        auto expression = to_visit.back()->clone();
        to_visit.pop_back();

        RequiredSourceColumnsVisitor::Data columns_context;
        RequiredSourceColumnsVisitor(columns_context).visit(expression);

        for (const auto & required_name : columns_context.requiredColumns())
        {
            String storage_name = resolveDefaultDependencyToStorage(required_name, columns_desc);
            if (storage_name.empty())
                continue;

            auto dependency_default = columns_desc.getDefault(storage_name);

            /// An `ALIAS` column is not physically stored, so it never appears in `expired_columns`.
            /// Expand it into the physical columns its expression reads instead. `visited_columns`
            /// guards against cycles in (mutually) recursive definitions.
            if (dependency_default && dependency_default->kind == ColumnDefaultKind::Alias && dependency_default->expression)
            {
                if (visited_columns.emplace(storage_name).second)
                    to_visit.push_back(dependency_default->expression);
                continue;
            }

            /// A physical column that has no values in any source is recomputed from its own
            /// `DEFAULT`/`MATERIALIZED` expression for all rows, so follow its dependencies too: the
            /// subcolumn we started from transitively reads whatever this expression reads.
            if (dependency_default && dependency_default->expression
                && columns_to_follow.contains(storage_name)
                && visited_columns.emplace(storage_name).second)
                to_visit.push_back(dependency_default->expression);

            result.emplace(std::move(storage_name));
        }
    }

    return result;
}

NameSet getNestedColumnsReconciledInSomePart(const NamesAndTypesList & storage_columns, const MergeTreeData::DataPartsVector & parts)
{
    std::unordered_map<String, Names> array_subcolumns_by_nested_name;
    for (const auto & storage_column : storage_columns)
    {
        auto split = Nested::splitName(storage_column.name);
        if (!split.second.empty() && isArray(storage_column.type))
            array_subcolumns_by_nested_name[split.first].push_back(storage_column.name);
    }

    NameSet result;
    for (const auto & part : parts)
    {
        const auto part_column_names = part->getColumns().getNameSet();
        for (const auto & [_, subcolumns] : array_subcolumns_by_nested_name)
        {
            if (subcolumns.size() < 2
                || !std::ranges::any_of(subcolumns, [&](const String & name) { return part_column_names.contains(name); }))
                continue;

            for (const auto & name : subcolumns)
                if (!part_column_names.contains(name))
                    result.insert(name);
        }
    }
    return result;
}

bool mayReadReconciledNestedDefault(
    const Names & required_columns,
    const NameSet & reconciled_columns,
    const ColumnsDescription & columns_desc,
    const NameSet & storage_column_names)
{
    if (reconciled_columns.empty())
        return false;

    for (const auto & name : required_columns)
    {
        /// A name that is not a storage column is a subcolumn of one, or a virtual column.
        auto name_in_storage = storage_column_names.contains(name)
            ? name
            : String(Nested::getColumnFromSubcolumn(name, storage_column_names));

        if (reconciled_columns.contains(name_in_storage))
            return true;

        auto column_default = columns_desc.getDefault(name_in_storage);
        if (column_default && column_default->expression)
        {
            for (const auto & dependency : collectDefaultStorageDependencies(column_default->expression, columns_desc, storage_column_names))
                if (reconciled_columns.contains(dependency))
                    return true;
        }
    }
    return false;
}

bool isStoredMaterializedColumnRecomputable(
    const String & column_name,
    const ColumnsDescription & columns_desc,
    const StorageInMemoryMetadata & metadata,
    const ContextPtr & context)
{
    auto column_default = columns_desc.getDefault(column_name);
    if (!column_default || column_default->kind != ColumnDefaultKind::Materialized || !column_default->expression)
        return false;

    if (metadata.getStorageColumnsRequiredForKeys().contains(column_name))
        return false;

    /// The node holds the expression with the `ALIAS` references expanded, so a non-deterministic
    /// function behind an alias is seen as well.
    MaterializedColumnDependencies dependencies(columns_desc, context);
    const auto * node = dependencies.findNode(column_name);
    return node && !node->reads_ephemeral && hasOnlyDeterministicFunctions(node->expression, context);
}

}
