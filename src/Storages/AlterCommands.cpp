#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/RenameColumnVisitor.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Common/typeid_cast.h>


#include <Parsers/queryToString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int LOGICAL_ERROR;
    extern const int DUPLICATE_COLUMN;
    extern const int NOT_IMPLEMENTED;
}


std::optional<AlterCommand> AlterCommand::parse(const ASTAlterCommand * command_ast, bool sanity_check_compression_codecs)
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();
    const CompressionCodecFactory & compression_codec_factory = CompressionCodecFactory::instance();

    if (command_ast->type == ASTAlterCommand::ADD_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::ADD_COLUMN;

        const auto & ast_col_decl = command_ast->col_decl->as<ASTColumnDeclaration &>();

        command.column_name = ast_col_decl.name;
        if (ast_col_decl.type)
        {
            command.data_type = data_type_factory.get(ast_col_decl.type);
        }
        if (ast_col_decl.default_expression)
        {
            command.default_kind = columnDefaultKindFromString(ast_col_decl.default_specifier);
            command.default_expression = ast_col_decl.default_expression;
        }

        if (ast_col_decl.comment)
        {
            const auto & ast_comment = typeid_cast<ASTLiteral &>(*ast_col_decl.comment);
            command.comment = ast_comment.value.get<String>();
        }

        if (ast_col_decl.codec)
            command.codec = compression_codec_factory.get(ast_col_decl.codec, command.data_type, sanity_check_compression_codecs);

        if (command_ast->column)
            command.after_column = getIdentifierName(command_ast->column);

        if (ast_col_decl.ttl)
            command.ttl = ast_col_decl.ttl;

        command.first = command_ast->first;
        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::DROP_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::DROP_COLUMN;
        command.column_name = getIdentifierName(command_ast->column);
        command.if_exists = command_ast->if_exists;
        if (command_ast->clear_column)
            command.clear = true;

        if (command_ast->partition)
            command.partition = command_ast->partition;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_COLUMN;

        const auto & ast_col_decl = command_ast->col_decl->as<ASTColumnDeclaration &>();

        command.column_name = ast_col_decl.name;
        if (ast_col_decl.type)
        {
            command.data_type = data_type_factory.get(ast_col_decl.type);
        }

        if (ast_col_decl.default_expression)
        {
            command.default_kind = columnDefaultKindFromString(ast_col_decl.default_specifier);
            command.default_expression = ast_col_decl.default_expression;
        }

        if (ast_col_decl.comment)
        {
            const auto & ast_comment = ast_col_decl.comment->as<ASTLiteral &>();
            command.comment.emplace(ast_comment.value.get<String>());
        }

        if (ast_col_decl.ttl)
            command.ttl = ast_col_decl.ttl;

        if (ast_col_decl.codec)
            command.codec = compression_codec_factory.get(ast_col_decl.codec, command.data_type, sanity_check_compression_codecs);

        if (command_ast->column)
            command.after_column = getIdentifierName(command_ast->column);

        command.first = command_ast->first;
        command.if_exists = command_ast->if_exists;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::COMMENT_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = COMMENT_COLUMN;
        command.column_name = getIdentifierName(command_ast->column);
        const auto & ast_comment = command_ast->comment->as<ASTLiteral &>();
        command.comment = ast_comment.value.get<String>();
        command.if_exists = command_ast->if_exists;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_ORDER_BY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_ORDER_BY;
        command.order_by = command_ast->order_by;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::ADD_INDEX)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.index_decl = command_ast->index_decl;
        command.type = AlterCommand::ADD_INDEX;

        const auto & ast_index_decl = command_ast->index_decl->as<ASTIndexDeclaration &>();

        command.index_name = ast_index_decl.name;

        if (command_ast->index)
            command.after_index_name = command_ast->index->as<ASTIdentifier &>().name;

        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::ADD_CONSTRAINT)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.constraint_decl = command_ast->constraint_decl;
        command.type = AlterCommand::ADD_CONSTRAINT;

        const auto & ast_constraint_decl = command_ast->constraint_decl->as<ASTConstraintDeclaration &>();

        command.constraint_name = ast_constraint_decl.name;

        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::DROP_CONSTRAINT)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.if_exists = command_ast->if_exists;
        command.type = AlterCommand::DROP_CONSTRAINT;
        command.constraint_name = command_ast->constraint->as<ASTIdentifier &>().name;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::DROP_INDEX)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::DROP_INDEX;
        command.index_name = command_ast->index->as<ASTIdentifier &>().name;
        command.if_exists = command_ast->if_exists;
        if (command_ast->clear_index)
            command.clear = true;

        if (command_ast->partition)
            command.partition = command_ast->partition;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_TTL)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_TTL;
        command.ttl = command_ast->ttl;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_SETTING)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_SETTING;
        command.settings_changes = command_ast->settings_changes->as<ASTSetQuery &>().changes;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_QUERY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_QUERY;
        command.select = command_ast->select;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::RENAME_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::RENAME_COLUMN;
        command.column_name = command_ast->column->as<ASTIdentifier &>().name;
        command.rename_to = command_ast->rename_to->as<ASTIdentifier &>().name;
        command.if_exists = command_ast->if_exists;
        return command;
    }
    else
        return {};
}


void AlterCommand::apply(StorageInMemoryMetadata & metadata, const Context & context) const
{
    if (type == ADD_COLUMN)
    {
        ColumnDescription column(column_name, data_type);
        if (default_expression)
        {
            column.default_desc.kind = default_kind;
            column.default_desc.expression = default_expression;
        }
        if (comment)
            column.comment = *comment;

        column.codec = codec;
        column.ttl = ttl;

        metadata.columns.add(column, after_column, first);

        /// Slow, because each time a list is copied
        metadata.columns.flattenNested();
    }
    else if (type == DROP_COLUMN)
    {
        /// Otherwise just clear data on disk
        if (!clear && !partition)
            metadata.columns.remove(column_name);
    }
    else if (type == MODIFY_COLUMN)
    {
        metadata.columns.modify(column_name, after_column, first, [&](ColumnDescription & column)
        {
            if (codec)
            {
                /// User doesn't specify data type, it means that datatype doesn't change
                /// let's use info about old type
                if (data_type == nullptr)
                    codec->useInfoAboutType(column.type);
                else /// use info about new DataType
                    codec->useInfoAboutType(data_type);

                column.codec = codec;
            }

            if (comment)
                column.comment = *comment;

            if (ttl)
                column.ttl = ttl;

            if (data_type)
                column.type = data_type;

            /// User specified default expression or changed
            /// datatype. We have to replace default.
            if (default_expression || data_type)
            {
                column.default_desc.kind = default_kind;
                column.default_desc.expression = default_expression;
            }
        });

    }
    else if (type == MODIFY_ORDER_BY)
    {
        auto & sorting_key = metadata.sorting_key;
        auto & primary_key = metadata.primary_key;
        if (primary_key.definition_ast == nullptr && sorting_key.definition_ast != nullptr)
        {
            /// Primary and sorting key become independent after this ALTER so
            /// we have to save the old ORDER BY expression as the new primary
            /// key.
            primary_key = KeyDescription::getKeyFromAST(sorting_key.definition_ast, metadata.columns, context);
        }

        /// Recalculate key with new order_by expression.
        sorting_key.recalculateWithNewAST(order_by, metadata.columns, context);
    }
    else if (type == COMMENT_COLUMN)
    {
        metadata.columns.modify(column_name,
            [&](ColumnDescription & column) { column.comment = *comment; });
    }
    else if (type == ADD_INDEX)
    {
        if (std::any_of(
                metadata.secondary_indices.cbegin(),
                metadata.secondary_indices.cend(),
                [this](const auto & index)
                {
                    return index.name == index_name;
                }))
        {
            if (if_not_exists)
                return;
            else
                throw Exception{"Cannot add index " + index_name + ": index with this name already exists",
                                ErrorCodes::ILLEGAL_COLUMN};
        }

        auto insert_it = metadata.secondary_indices.end();

        if (!after_index_name.empty())
        {
            insert_it = std::find_if(
                    metadata.secondary_indices.begin(),
                    metadata.secondary_indices.end(),
                    [this](const auto & index)
                    {
                        return index.name == after_index_name;
                    });

            if (insert_it == metadata.secondary_indices.end())
                throw Exception("Wrong index name. Cannot find index " + backQuote(after_index_name) + " to insert after.",
                        ErrorCodes::BAD_ARGUMENTS);

            ++insert_it;
        }

        metadata.secondary_indices.emplace(insert_it, IndexDescription::getIndexFromAST(index_decl, metadata.columns, context));
    }
    else if (type == DROP_INDEX)
    {
        if (!partition && !clear)
        {
            auto erase_it = std::find_if(
                    metadata.secondary_indices.begin(),
                    metadata.secondary_indices.end(),
                    [this](const auto & index)
                    {
                        return index.name == index_name;
                    });

            if (erase_it == metadata.secondary_indices.end())
            {
                if (if_exists)
                    return;
                throw Exception("Wrong index name. Cannot find index " + backQuote(index_name) + " to drop.", ErrorCodes::BAD_ARGUMENTS);
            }

            metadata.secondary_indices.erase(erase_it);
        }
    }
    else if (type == ADD_CONSTRAINT)
    {
        if (std::any_of(
                metadata.constraints.constraints.cbegin(),
                metadata.constraints.constraints.cend(),
                [this](const ASTPtr & constraint_ast)
                {
                    return constraint_ast->as<ASTConstraintDeclaration &>().name == constraint_name;
                }))
        {
            if (if_not_exists)
                return;
            throw Exception("Cannot add constraint " + constraint_name + ": constraint with this name already exists",
                        ErrorCodes::ILLEGAL_COLUMN);
        }

        auto insert_it = metadata.constraints.constraints.end();

        metadata.constraints.constraints.emplace(insert_it, std::dynamic_pointer_cast<ASTConstraintDeclaration>(constraint_decl));
    }
    else if (type == DROP_CONSTRAINT)
    {
        auto erase_it = std::find_if(
                metadata.constraints.constraints.begin(),
                metadata.constraints.constraints.end(),
                [this](const ASTPtr & constraint_ast)
                {
                    return constraint_ast->as<ASTConstraintDeclaration &>().name == constraint_name;
                });

        if (erase_it == metadata.constraints.constraints.end())
        {
            if (if_exists)
                return;
            throw Exception("Wrong constraint name. Cannot find constraint `" + constraint_name + "` to drop.",
                    ErrorCodes::BAD_ARGUMENTS);
        }
        metadata.constraints.constraints.erase(erase_it);
    }
    else if (type == MODIFY_TTL)
    {
        metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(ttl, metadata.columns, context, metadata.primary_key);
    }
    else if (type == MODIFY_QUERY)
    {
        metadata.select = SelectQueryDescription::getSelectQueryFromASTForMatView(select, context);
    }
    else if (type == MODIFY_SETTING)
    {
        auto & settings_from_storage = metadata.settings_changes->as<ASTSetQuery &>().changes;
        for (const auto & change : settings_changes)
        {
            auto finder = [&change](const SettingChange & c) { return c.name == change.name; };
            auto it = std::find_if(settings_from_storage.begin(), settings_from_storage.end(), finder);

            if (it != settings_from_storage.end())
                it->value = change.value;
            else
                settings_from_storage.push_back(change);
        }
    }
    else if (type == RENAME_COLUMN)
    {
        metadata.columns.rename(column_name, rename_to);
        RenameColumnData rename_data{column_name, rename_to};
        RenameColumnVisitor rename_visitor(rename_data);
        for (const auto & column : metadata.columns)
        {
            metadata.columns.modify(column.name, [&](ColumnDescription & column_to_modify)
            {
                if (column_to_modify.default_desc.expression)
                    rename_visitor.visit(column_to_modify.default_desc.expression);
                if (column_to_modify.ttl)
                    rename_visitor.visit(column_to_modify.ttl);
            });
        }
        if (metadata.table_ttl.definition_ast)
            rename_visitor.visit(metadata.table_ttl.definition_ast);

        for (auto & constraint : metadata.constraints.constraints)
            rename_visitor.visit(constraint);

        if (metadata.isSortingKeyDefined())
            rename_visitor.visit(metadata.sorting_key.definition_ast);

        if (metadata.isPrimaryKeyDefined())
            rename_visitor.visit(metadata.primary_key.definition_ast);

        if (metadata.isSamplingKeyDefined())
            rename_visitor.visit(metadata.sampling_key.definition_ast);

        if (metadata.isPartitionKeyDefined())
            rename_visitor.visit(metadata.partition_key.definition_ast);

        for (auto & index : metadata.secondary_indices)
            rename_visitor.visit(index.definition_ast);
    }
    else
        throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
}

namespace
{

/// If true, then in order to ALTER the type of the column from the type from to the type to
/// we don't need to rewrite the data, we only need to update metadata and columns.txt in part directories.
/// The function works for Arrays and Nullables of the same structure.
bool isMetadataOnlyConversion(const IDataType * from, const IDataType * to)
{
    if (from->equals(*to))
        return true;

    if (const auto * from_enum8 = typeid_cast<const DataTypeEnum8 *>(from))
    {
        if (const auto * to_enum8 = typeid_cast<const DataTypeEnum8 *>(to))
            return to_enum8->contains(*from_enum8);
    }

    if (const auto * from_enum16 = typeid_cast<const DataTypeEnum16 *>(from))
    {
        if (const auto * to_enum16 = typeid_cast<const DataTypeEnum16 *>(to))
            return to_enum16->contains(*from_enum16);
    }

    static const std::unordered_multimap<std::type_index, const std::type_info &> ALLOWED_CONVERSIONS =
        {
            { typeid(DataTypeEnum8),    typeid(DataTypeInt8)     },
            { typeid(DataTypeEnum16),   typeid(DataTypeInt16)    },
            { typeid(DataTypeDateTime), typeid(DataTypeUInt32)   },
            { typeid(DataTypeUInt32),   typeid(DataTypeDateTime) },
            { typeid(DataTypeDate),     typeid(DataTypeUInt16)   },
            { typeid(DataTypeUInt16),   typeid(DataTypeDate)     },
        };

    while (true)
    {
        auto it_range = ALLOWED_CONVERSIONS.equal_range(typeid(*from));
        for (auto it = it_range.first; it != it_range.second; ++it)
        {
            if (it->second == typeid(*to))
                return true;
        }

        const auto * arr_from = typeid_cast<const DataTypeArray *>(from);
        const auto * arr_to = typeid_cast<const DataTypeArray *>(to);
        if (arr_from && arr_to)
        {
            from = arr_from->getNestedType().get();
            to = arr_to->getNestedType().get();
            continue;
        }

        const auto * nullable_from = typeid_cast<const DataTypeNullable *>(from);
        const auto * nullable_to = typeid_cast<const DataTypeNullable *>(to);
        if (nullable_from && nullable_to)
        {
            from = nullable_from->getNestedType().get();
            to = nullable_to->getNestedType().get();
            continue;
        }

        return false;
    }
}

}

bool AlterCommand::isSettingsAlter() const
{
    return type == MODIFY_SETTING;
}

bool AlterCommand::isRequireMutationStage(const StorageInMemoryMetadata & metadata) const
{
    if (ignore)
        return false;

    if (type == DROP_COLUMN || type == DROP_INDEX || type == RENAME_COLUMN)
        return true;

    if (type != MODIFY_COLUMN || data_type == nullptr)
        return false;

    for (const auto & column : metadata.columns.getAllPhysical())
    {
        if (column.name == column_name && !isMetadataOnlyConversion(column.type.get(), data_type.get()))
            return true;
    }
    return false;
}

bool AlterCommand::isCommentAlter() const
{
    if (type == COMMENT_COLUMN)
    {
        return true;
    }
    else if (type == MODIFY_COLUMN)
    {
        return comment.has_value()
            && codec == nullptr
            && data_type == nullptr
            && default_expression == nullptr
            && ttl == nullptr;
    }
    return false;
}

bool AlterCommand::isTTLAlter(const StorageInMemoryMetadata & metadata) const
{
    if (type == MODIFY_TTL)
        return true;

    if (!ttl || type != MODIFY_COLUMN)
        return false;

    bool ttl_changed = true;
    for (const auto & [name, ttl_ast] : metadata.columns.getColumnTTLs())
    {
        if (name == column_name && queryToString(*ttl) == queryToString(*ttl_ast))
        {
            ttl_changed = false;
            break;
        }
    }

    return ttl_changed;
}

std::optional<MutationCommand> AlterCommand::tryConvertToMutationCommand(StorageInMemoryMetadata & metadata, const Context & context) const
{
    if (!isRequireMutationStage(metadata))
        return {};

    MutationCommand result;

    if (type == MODIFY_COLUMN)
    {
        result.type = MutationCommand::Type::READ_COLUMN;
        result.column_name = column_name;
        result.data_type = data_type;
        result.predicate = nullptr;
    }
    else if (type == DROP_COLUMN)
    {
        result.type = MutationCommand::Type::DROP_COLUMN;
        result.column_name = column_name;
        if (clear)
            result.clear = true;
        if (partition)
            result.partition = partition;
        result.predicate = nullptr;
    }
    else if (type == DROP_INDEX)
    {
        result.type = MutationCommand::Type::DROP_INDEX;
        result.column_name = index_name;
        if (clear)
            result.clear = true;
        if (partition)
            result.partition = partition;

        result.predicate = nullptr;
    }
    else if (type == RENAME_COLUMN)
    {
        result.type = MutationCommand::Type::RENAME_COLUMN;
        result.column_name = column_name;
        result.rename_to = rename_to;
    }

    result.ast = ast->clone();
    apply(metadata, context);
    return result;
}


String alterTypeToString(const AlterCommand::Type type)
{
    switch (type)
    {
    case AlterCommand::Type::ADD_COLUMN:
        return "ADD COLUMN";
    case AlterCommand::Type::ADD_CONSTRAINT:
        return "ADD CONSTRAINT";
    case AlterCommand::Type::ADD_INDEX:
        return "ADD INDEX";
    case AlterCommand::Type::COMMENT_COLUMN:
        return "COMMENT COLUMN";
    case AlterCommand::Type::DROP_COLUMN:
        return "DROP COLUMN";
    case AlterCommand::Type::DROP_CONSTRAINT:
        return "DROP CONSTRAINT";
    case AlterCommand::Type::DROP_INDEX:
        return "DROP INDEX";
    case AlterCommand::Type::MODIFY_COLUMN:
        return "MODIFY COLUMN";
    case AlterCommand::Type::MODIFY_ORDER_BY:
        return "MODIFY ORDER BY";
    case AlterCommand::Type::MODIFY_TTL:
        return "MODIFY TTL";
    case AlterCommand::Type::MODIFY_SETTING:
        return "MODIFY SETTING";
    case AlterCommand::Type::MODIFY_QUERY:
        return "MODIFY QUERY";
    case AlterCommand::Type::RENAME_COLUMN:
        return "RENAME COLUMN";
    }
    __builtin_unreachable();
}

void AlterCommands::apply(StorageInMemoryMetadata & metadata, const Context & context) const
{
    if (!prepared)
        throw DB::Exception("Alter commands is not prepared. Cannot apply. It's a bug", ErrorCodes::LOGICAL_ERROR);

    auto metadata_copy = metadata;
    for (const AlterCommand & command : *this)
        if (!command.ignore)
            command.apply(metadata_copy, context);

    /// Changes in columns may lead to changes in keys expression.
    metadata_copy.sorting_key.recalculateWithNewAST(metadata_copy.sorting_key.definition_ast, metadata_copy.columns, context);
    if (metadata_copy.primary_key.definition_ast != nullptr)
    {
        metadata_copy.primary_key.recalculateWithNewAST(metadata_copy.primary_key.definition_ast, metadata_copy.columns, context);
    }
    else
    {
        metadata_copy.primary_key = KeyDescription::getKeyFromAST(metadata_copy.sorting_key.definition_ast, metadata_copy.columns, context);
        metadata_copy.primary_key.definition_ast = nullptr;
    }

    /// And in partition key expression
    if (metadata_copy.partition_key.definition_ast != nullptr)
        metadata_copy.partition_key.recalculateWithNewAST(metadata_copy.partition_key.definition_ast, metadata_copy.columns, context);

    /// Changes in columns may lead to changes in secondary indices
    for (auto & index : metadata_copy.secondary_indices)
        index = IndexDescription::getIndexFromAST(index.definition_ast, metadata_copy.columns, context);

    /// Changes in columns may lead to changes in TTL expressions.
    auto column_ttl_asts = metadata_copy.columns.getColumnTTLs();
    for (const auto & [name, ast] : column_ttl_asts)
    {
        auto new_ttl_entry = TTLDescription::getTTLFromAST(ast, metadata_copy.columns, context, metadata_copy.primary_key);
        metadata_copy.column_ttls_by_name[name] = new_ttl_entry;
    }

    if (metadata_copy.table_ttl.definition_ast != nullptr)
        metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
            metadata_copy.table_ttl.definition_ast, metadata_copy.columns, context, metadata_copy.primary_key);

    metadata = std::move(metadata_copy);
}


void AlterCommands::prepare(const StorageInMemoryMetadata & metadata)
{
    auto columns = metadata.columns;

    for (size_t i = 0; i < size(); ++i)
    {
        auto & command = (*this)[i];
        bool has_column = columns.has(command.column_name) || columns.hasNested(command.column_name);
        if (command.type == AlterCommand::MODIFY_COLUMN)
        {
            if (!has_column && command.if_exists)
                command.ignore = true;

            if (has_column && command.data_type)
            {
                auto column_from_table = columns.get(command.column_name);
                if (!command.default_expression && column_from_table.default_desc.expression)
                {
                    command.default_kind = column_from_table.default_desc.kind;
                    command.default_expression = column_from_table.default_desc.expression;
                }
            }
        }
        else if (command.type == AlterCommand::ADD_COLUMN)
        {
            if (has_column && command.if_not_exists)
                command.ignore = true;
        }
        else if (command.type == AlterCommand::DROP_COLUMN
                || command.type == AlterCommand::COMMENT_COLUMN
                || command.type == AlterCommand::RENAME_COLUMN)
        {
            if (!has_column && command.if_exists)
                command.ignore = true;
        }
    }
    prepared = true;
}

void AlterCommands::validate(const StorageInMemoryMetadata & metadata, const Context & context) const
{
    auto all_columns = metadata.columns;
    /// Default expression for all added/modified columns
    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();
    NameSet modified_columns, renamed_columns;
    for (size_t i = 0; i < size(); ++i)
    {
        const auto & command = (*this)[i];

        const auto & column_name = command.column_name;
        if (command.type == AlterCommand::ADD_COLUMN)
        {
            if (all_columns.has(column_name) || all_columns.hasNested(column_name))
            {
                if (!command.if_not_exists)
                    throw Exception{"Cannot add column " + backQuote(column_name) + ": column with this name already exists",
                                    ErrorCodes::DUPLICATE_COLUMN};
                else
                    continue;
            }

            if (!command.data_type)
                throw Exception{"Data type have to be specified for column " + backQuote(column_name) + " to add",
                                ErrorCodes::BAD_ARGUMENTS};

            all_columns.add(ColumnDescription(column_name, command.data_type));
        }
        else if (command.type == AlterCommand::MODIFY_COLUMN)
        {
            if (!all_columns.has(column_name))
            {
                if (!command.if_exists)
                    throw Exception{"Wrong column name. Cannot find column " + backQuote(column_name) + " to modify",
                                    ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK};
                else
                    continue;
            }

            if (renamed_columns.count(column_name))
                throw Exception{"Cannot rename and modify the same column " + backQuote(column_name) + " in a single ALTER query",
                                ErrorCodes::NOT_IMPLEMENTED};
            modified_columns.emplace(column_name);
        }
        else if (command.type == AlterCommand::DROP_COLUMN)
        {
            if (all_columns.has(command.column_name) || all_columns.hasNested(command.column_name))
            {
                if (!command.clear) /// CLEAR column is Ok even if there are dependencies.
                {
                    /// Check if we are going to DROP a column that some other columns depend on.
                    for (const ColumnDescription & column : all_columns)
                    {
                        const auto & default_expression = column.default_desc.expression;
                        if (default_expression)
                        {
                            ASTPtr query = default_expression->clone();
                            auto syntax_result = TreeRewriter(context).analyze(query, all_columns.getAll());
                            const auto actions = ExpressionAnalyzer(query, syntax_result, context).getActions(true);
                            const auto required_columns = actions->getRequiredColumns();

                            if (required_columns.end() != std::find(required_columns.begin(), required_columns.end(), command.column_name))
                                throw Exception("Cannot drop column " + backQuote(command.column_name)
                                        + ", because column " + backQuote(column.name) + " depends on it",
                                    ErrorCodes::ILLEGAL_COLUMN);
                        }
                    }
                }
                all_columns.remove(command.column_name);
            }
            else if (!command.if_exists)
                throw Exception(
                    "Wrong column name. Cannot find column " + backQuote(command.column_name) + " to drop",
                    ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
        }
        else if (command.type == AlterCommand::COMMENT_COLUMN)
        {
            if (!all_columns.has(command.column_name))
            {
                if (!command.if_exists)
                    throw Exception{"Wrong column name. Cannot find column " + backQuote(command.column_name) + " to comment",
                                    ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK};
            }
        }
        else if (command.type == AlterCommand::MODIFY_SETTING)
        {
            if (metadata.settings_changes == nullptr)
                throw Exception{"Cannot alter settings, because table engine doesn't support settings changes", ErrorCodes::BAD_ARGUMENTS};
        }
        else if (command.type == AlterCommand::RENAME_COLUMN)
        {
           for (size_t j = i + 1; j < size(); ++j)
           {
               auto next_command = (*this)[j];
               if (next_command.type == AlterCommand::RENAME_COLUMN)
               {
                   if (next_command.column_name == command.rename_to)
                       throw Exception{"Transitive renames in a single ALTER query are not allowed (don't make sense)",
                                                            ErrorCodes::NOT_IMPLEMENTED};
                   else if (next_command.column_name == command.column_name)
                       throw Exception{"Cannot rename column '" + backQuote(command.column_name)
                                           + "' to two different names in a single ALTER query",
                                       ErrorCodes::BAD_ARGUMENTS};
               }
           }

            /// TODO Implement nested rename
            if (all_columns.hasNested(command.column_name))
            {
                throw Exception{"Cannot rename whole Nested struct", ErrorCodes::NOT_IMPLEMENTED};
            }

            if (!all_columns.has(command.column_name))
            {
                if (!command.if_exists)
                    throw Exception{"Wrong column name. Cannot find column " + backQuote(command.column_name) + " to rename",
                                    ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK};
                else
                    continue;
            }

            if (all_columns.has(command.rename_to))
                throw Exception{"Cannot rename to " + backQuote(command.rename_to) + ": column with this name already exists",
                                ErrorCodes::DUPLICATE_COLUMN};

            if (modified_columns.count(column_name))
                throw Exception{"Cannot rename and modify the same column " + backQuote(column_name) + " in a single ALTER query",
                                ErrorCodes::NOT_IMPLEMENTED};

            String from_nested_table_name = Nested::extractTableName(command.column_name);
            String to_nested_table_name = Nested::extractTableName(command.rename_to);
            bool from_nested = from_nested_table_name != command.column_name;
            bool to_nested = to_nested_table_name != command.rename_to;

            if (from_nested && to_nested)
            {
                if (from_nested_table_name != to_nested_table_name)
                    throw Exception{"Cannot rename column from one nested name to another", ErrorCodes::BAD_ARGUMENTS};
            }
            else if (!from_nested && !to_nested)
            {
                all_columns.rename(command.column_name, command.rename_to);
                renamed_columns.emplace(command.column_name);
                renamed_columns.emplace(command.rename_to);
            }
            else
            {
                throw Exception{"Cannot rename column from nested struct to normal column and vice versa", ErrorCodes::BAD_ARGUMENTS};
            }
        }

        /// Collect default expressions for MODIFY and ADD comands
        if (command.type == AlterCommand::MODIFY_COLUMN || command.type == AlterCommand::ADD_COLUMN)
        {
            if (command.default_expression)
            {
                /// If we modify default, but not type
                if (!command.data_type)
                {
                    default_expr_list->children.emplace_back(setAlias(command.default_expression->clone(), column_name));
                }
                else
                {
                    const auto & final_column_name = column_name;
                    const auto tmp_column_name = final_column_name + "_tmp";
                    const auto data_type_ptr = command.data_type;


                    default_expr_list->children.emplace_back(setAlias(
                        addTypeConversionToAST(std::make_shared<ASTIdentifier>(tmp_column_name), data_type_ptr->getName()),
                        final_column_name));

                    default_expr_list->children.emplace_back(setAlias(command.default_expression->clone(), tmp_column_name));
                }
            } /// if we change data type for column with default
            else if (all_columns.has(column_name) && command.data_type)
            {
                auto column_in_table = all_columns.get(column_name);
                /// Column doesn't have a default, nothing to check
                if (!column_in_table.default_desc.expression)
                    continue;

                const auto & final_column_name = column_name;
                const auto tmp_column_name = final_column_name + "_tmp";
                const auto data_type_ptr = command.data_type;


                default_expr_list->children.emplace_back(setAlias(
                    addTypeConversionToAST(std::make_shared<ASTIdentifier>(tmp_column_name), data_type_ptr->getName()), final_column_name));

                default_expr_list->children.emplace_back(setAlias(column_in_table.default_desc.expression->clone(), tmp_column_name));
            }
        }
    }

    if (all_columns.empty())
        throw Exception{"Cannot DROP or CLEAR all columns", ErrorCodes::BAD_ARGUMENTS};

    validateColumnsDefaultsAndGetSampleBlock(default_expr_list, all_columns.getAll(), context);
}

bool AlterCommands::isSettingsAlter() const
{
    return std::all_of(begin(), end(), [](const AlterCommand & c) { return c.isSettingsAlter(); });
}

bool AlterCommands::isCommentAlter() const
{
    return std::all_of(begin(), end(), [](const AlterCommand & c) { return c.isCommentAlter(); });
}

static MutationCommand createMaterializeTTLCommand()
{
    MutationCommand command;
    auto ast = std::make_shared<ASTAlterCommand>();
    ast->type = ASTAlterCommand::MATERIALIZE_TTL;
    command.type = MutationCommand::MATERIALIZE_TTL;
    command.ast = std::move(ast);
    return command;
}

MutationCommands AlterCommands::getMutationCommands(StorageInMemoryMetadata metadata, bool materialize_ttl, const Context & context) const
{
    MutationCommands result;
    for (const auto & alter_cmd : *this)
        if (auto mutation_cmd = alter_cmd.tryConvertToMutationCommand(metadata, context); mutation_cmd)
            result.push_back(*mutation_cmd);

    if (materialize_ttl)
    {
        for (const auto & alter_cmd : *this)
        {
            if (alter_cmd.isTTLAlter(metadata))
            {
                result.push_back(createMaterializeTTLCommand());
                break;
            }
        }
    }

    return result;
}

}
