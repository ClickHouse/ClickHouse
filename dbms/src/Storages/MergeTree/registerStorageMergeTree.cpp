#include <Storages/StorageFactory.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>


namespace DB
{

static void create(
    ASTs & args,
    const String & data_path,
    const String & table_name,
    const String & database_name,
    Context & local_context,
    Context & context,
    const NamesAndTypesList & columns,
    const NamesAndTypesList & materialized_columns,
    const NamesAndTypesList & alias_columns,
    const ColumnDefaults & column_defaults,
    bool attach,
    bool has_force_restore_data_flag)
{
    /** [Replicated][|Summing|Collapsing|Aggregating|Replacing|Graphite]MergeTree (2 * 7 combinations) engines
        * The argument for the engine should be:
        *  - (for Replicated) The path to the table in ZooKeeper
        *  - (for Replicated) Replica name in ZooKeeper
        *  - the name of the column with the date;
        *  - (optional) expression for sampling
        *     (the query with `SAMPLE x` will select rows that have a lower value in this column than `x * UINT32_MAX`);
        *  - an expression for sorting (either a scalar expression or a tuple of several);
        *  - index_granularity;
        *  - (for Collapsing) the name of Int8 column that contains `sign` type with the change of "visit" (taking values 1 and -1).
        * For example: ENGINE = ReplicatedCollapsingMergeTree('/tables/mytable', 'rep02', EventDate, (CounterID, EventDate, intHash32(UniqID), VisitID), 8192, Sign).
        *  - (for Summing, optional) a tuple of columns to be summed. If not specified, all numeric columns that are not included in the primary key are used.
        *  - (for Replacing, optional) the column name of one of the UInt types, which stands for "version"
        * For example: ENGINE = ReplicatedCollapsingMergeTree('/tables/mytable', 'rep02', EventDate, (CounterID, EventDate, intHash32(UniqID), VisitID), 8192, Sign).
        *  - (for Graphite) the parameter name in config file with settings of thinning rules.
        *
        * MergeTree(date, [sample_key], primary_key, index_granularity)
        * CollapsingMergeTree(date, [sample_key], primary_key, index_granularity, sign)
        * SummingMergeTree(date, [sample_key], primary_key, index_granularity, [columns_to_sum])
        * AggregatingMergeTree(date, [sample_key], primary_key, index_granularity)
        * ReplacingMergeTree(date, [sample_key], primary_key, index_granularity, [version_column])
        * GraphiteMergeTree(date, [sample_key], primary_key, index_granularity, 'config_element')
        *
        * Alternatively, you can specify:
        *  - Partitioning expression in the PARTITION BY clause;
        *  - Primary key in the ORDER BY clause;
        *  - Sampling expression in the SAMPLE BY clause;
        *  - Additional MergeTreeSettings in the SETTINGS clause;
        */

    bool is_extended_storage_def =
        storage_def.partition_by || storage_def.order_by || storage_def.sample_by || storage_def.settings;

    String name_part = name.substr(0, name.size() - strlen("MergeTree"));

    bool replicated = startsWith(name_part, "Replicated");
    if (replicated)
        name_part = name_part.substr(strlen("Replicated"));

    MergeTreeData::MergingParams merging_params;
    merging_params.mode = MergeTreeData::MergingParams::Ordinary;

    if (name_part == "Collapsing")
        merging_params.mode = MergeTreeData::MergingParams::Collapsing;
    else if (name_part == "Summing")
        merging_params.mode = MergeTreeData::MergingParams::Summing;
    else if (name_part == "Aggregating")
        merging_params.mode = MergeTreeData::MergingParams::Aggregating;
    else if (name_part == "Replacing")
        merging_params.mode = MergeTreeData::MergingParams::Replacing;
    else if (name_part == "Graphite")
        merging_params.mode = MergeTreeData::MergingParams::Graphite;
    else if (!name_part.empty())
        throw Exception(
            "Unknown storage " + name + getMergeTreeVerboseHelp(is_extended_storage_def),
            ErrorCodes::UNKNOWN_STORAGE);

    /// NOTE Quite complicated.

    size_t min_num_params = 0;
    size_t max_num_params = 0;
    String needed_params;

    auto add_mandatory_param = [&](const char * desc)
    {
        ++min_num_params;
        ++max_num_params;
        needed_params += needed_params.empty() ? "\n" : ",\n";
        needed_params += desc;
    };
    auto add_optional_param = [&](const char * desc)
    {
        ++max_num_params;
        needed_params += needed_params.empty() ? "\n" : ",\n[";
        needed_params += desc;
        needed_params += "]";
    };

    if (replicated)
    {
        add_mandatory_param("path in ZooKeeper");
        add_mandatory_param("replica name");
    }

    if (!is_extended_storage_def)
    {
        add_mandatory_param("name of column with date");
        add_optional_param("sampling element of primary key");
        add_mandatory_param("primary key expression");
        add_mandatory_param("index granularity");
    }

    switch (merging_params.mode)
    {
    default:
        break;
    case MergeTreeData::MergingParams::Summing:
        add_optional_param("list of columns to sum");
        break;
    case MergeTreeData::MergingParams::Replacing:
        add_optional_param("version");
        break;
    case MergeTreeData::MergingParams::Collapsing:
        add_mandatory_param("sign column");
        break;
    case MergeTreeData::MergingParams::Graphite:
        add_mandatory_param("'config_element_for_graphite_schema'");
        break;
    }

    if (args.size() < min_num_params || args.size() > max_num_params)
    {
        String msg;
        if (is_extended_storage_def)
            msg += "With extended storage definition syntax storage " + name + " requires ";
        else
            msg += "Storage " + name + " requires ";

        if (max_num_params)
        {
            if (min_num_params == max_num_params)
                msg += toString(min_num_params) + " parameters: ";
            else
                msg += toString(min_num_params) + " to " + toString(max_num_params) + " parameters: ";
            msg += needed_params;
        }
        else
            msg += "no parameters";

        msg += getMergeTreeVerboseHelp(is_extended_storage_def);

        throw Exception(msg, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    /// For Replicated.
    String zookeeper_path;
    String replica_name;

    if (replicated)
    {
        auto ast = typeid_cast<ASTLiteral *>(&*args[0]);
        if (ast && ast->value.getType() == Field::Types::String)
            zookeeper_path = safeGet<String>(ast->value);
        else
            throw Exception(
                "Path in ZooKeeper must be a string literal" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);

        ast = typeid_cast<ASTLiteral *>(&*args[1]);
        if (ast && ast->value.getType() == Field::Types::String)
            replica_name = safeGet<String>(ast->value);
        else
            throw Exception(
                "Replica name must be a string literal" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);

        if (replica_name.empty())
            throw Exception(
                "No replica name in config" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::NO_REPLICA_NAME_GIVEN);

        args.erase(args.begin(), args.begin() + 2);
    }

    if (merging_params.mode == MergeTreeData::MergingParams::Collapsing)
    {
        if (auto ast = typeid_cast<ASTIdentifier *>(&*args.back()))
            merging_params.sign_column = ast->name;
        else
            throw Exception(
                "Sign column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);

        args.pop_back();
    }
    else if (merging_params.mode == MergeTreeData::MergingParams::Replacing)
    {
        /// If the last element is not index_granularity or replica_name (a literal), then this is the name of the version column.
        if (!args.empty() && !typeid_cast<const ASTLiteral *>(&*args.back()))
        {
            if (auto ast = typeid_cast<ASTIdentifier *>(&*args.back()))
                merging_params.version_column = ast->name;
            else
                throw Exception(
                    "Version column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                    ErrorCodes::BAD_ARGUMENTS);

            args.pop_back();
        }
    }
    else if (merging_params.mode == MergeTreeData::MergingParams::Summing)
    {
        /// If the last element is not index_granularity or replica_name (a literal), then this is a list of summable columns.
        if (!args.empty() && !typeid_cast<const ASTLiteral *>(&*args.back()))
        {
            merging_params.columns_to_sum = extractColumnNames(args.back());
            args.pop_back();
        }
    }
    else if (merging_params.mode == MergeTreeData::MergingParams::Graphite)
    {
        String graphite_config_name;
        String error_msg = "Last parameter of GraphiteMergeTree must be name (in single quotes) of element in configuration file with Graphite options";
        error_msg += getMergeTreeVerboseHelp(is_extended_storage_def);

        if (auto ast = typeid_cast<ASTLiteral *>(&*args.back()))
        {
            if (ast->value.getType() != Field::Types::String)
                throw Exception(error_msg, ErrorCodes::BAD_ARGUMENTS);

            graphite_config_name = ast->value.get<String>();
        }
        else
            throw Exception(error_msg, ErrorCodes::BAD_ARGUMENTS);

        args.pop_back();
        setGraphitePatternsFromConfig(context, graphite_config_name, merging_params.graphite_params);
    }

    String date_column_name;
    ASTPtr partition_expr_list;
    ASTPtr primary_expr_list;
    ASTPtr sampling_expression;
    MergeTreeSettings storage_settings = context.getMergeTreeSettings();

    if (is_extended_storage_def)
    {
        if (storage_def.partition_by)
            partition_expr_list = extractKeyExpressionList(*storage_def.partition_by);

        if (storage_def.order_by)
            primary_expr_list = extractKeyExpressionList(*storage_def.order_by);

        if (storage_def.sample_by)
            sampling_expression = storage_def.sample_by->ptr();

        storage_settings.loadFromQuery(storage_def);
    }
    else
    {
        /// If there is an expression for sampling. MergeTree(date, [sample_key], primary_key, index_granularity)
        if (args.size() == 4)
        {
            sampling_expression = args[1];
            args.erase(args.begin() + 1);
        }

        /// Now only three parameters remain - date (or partitioning expression), primary_key, index_granularity.

        if (auto ast = typeid_cast<ASTIdentifier *>(args[0].get()))
            date_column_name = ast->name;
        else
            throw Exception(
                "Date column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);

        primary_expr_list = extractKeyExpressionList(*args[1]);

        auto ast = typeid_cast<ASTLiteral *>(&*args.back());
        if (ast && ast->value.getType() == Field::Types::UInt64)
            storage_settings.index_granularity = safeGet<UInt64>(ast->value);
        else
            throw Exception(
                "Index granularity must be a positive integer" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);
    }

    if (replicated)
        return StorageReplicatedMergeTree::create(
            zookeeper_path, replica_name, attach, data_path, database_name, table_name,
            columns, materialized_columns, alias_columns, column_defaults,
            context, primary_expr_list, date_column_name, partition_expr_list,
            sampling_expression, merging_params, storage_settings,
            has_force_restore_data_flag);
    else
        return StorageMergeTree::create(
            data_path, database_name, table_name,
            columns, materialized_columns, alias_columns, column_defaults, attach,
            context, primary_expr_list, date_column_name, partition_expr_list,
            sampling_expression, merging_params, storage_settings,
            has_force_restore_data_flag);
}


void registerStorageMergeTree(StorageFactory & factory)
{
    factory.registerStorage("MergeTree", create);
    factory.registerStorage("CollapsingMergeTree", create);
    factory.registerStorage("ReplacingMergeTree", create);
    factory.registerStorage("AggregatingMergeTree", create);
    factory.registerStorage("SummingMergeTree", create);
    factory.registerStorage("GraphiteMergeTree", create);

    factory.registerStorage("ReplicatedMergeTree", create);
    factory.registerStorage("ReplicatedCollapsingMergeTree", create);
    factory.registerStorage("ReplicatedReplacingMergeTree", create);
    factory.registerStorage("ReplicatedAggregatingMergeTree", create);
    factory.registerStorage("ReplicatedSummingMergeTree", create);
    factory.registerStorage("ReplicatedGraphiteMergeTree", create);
}

}
