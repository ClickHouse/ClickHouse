#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{
StorageInMemoryMetadata::StorageInMemoryMetadata(
    ColumnsDescription columns_,
    IndicesDescription indices_,
    ConstraintsDescription constraints_)
    : columns(std::move(columns_))
    , indices(std::move(indices_))
    , constraints(std::move(constraints_))
{
}

StorageInMemoryMetadata::StorageInMemoryMetadata(const StorageInMemoryMetadata & other)
    : columns(other.columns)
    , indices(other.indices)
    , constraints(other.constraints)
{
    if (other.partition_by_ast)
        partition_by_ast = other.partition_by_ast->clone();
    if (other.order_by_ast)
        order_by_ast = other.order_by_ast->clone();
    if (other.primary_key_ast)
        primary_key_ast = other.primary_key_ast->clone();
    if (other.ttl_for_table_ast)
        ttl_for_table_ast = other.ttl_for_table_ast->clone();
    if (other.sample_by_ast)
        sample_by_ast = other.sample_by_ast->clone();
    if (other.settings_ast)
        settings_ast = other.settings_ast->clone();
    if (other.select)
        select = other.select->clone();
}

StorageInMemoryMetadata & StorageInMemoryMetadata::operator=(const StorageInMemoryMetadata & other)
{
    if (this == &other)
        return *this;

    columns = other.columns;
    indices = other.indices;
    constraints = other.constraints;

    if (other.partition_by_ast)
        partition_by_ast = other.partition_by_ast->clone();
    else
        partition_by_ast.reset();

    if (other.order_by_ast)
        order_by_ast = other.order_by_ast->clone();
    else
        order_by_ast.reset();

    if (other.primary_key_ast)
        primary_key_ast = other.primary_key_ast->clone();
    else
        primary_key_ast.reset();

    if (other.ttl_for_table_ast)
        ttl_for_table_ast = other.ttl_for_table_ast->clone();
    else
        ttl_for_table_ast.reset();

    if (other.sample_by_ast)
        sample_by_ast = other.sample_by_ast->clone();
    else
        sample_by_ast.reset();

    if (other.settings_ast)
        settings_ast = other.settings_ast->clone();
    else
        settings_ast.reset();

    if (other.select)
        select = other.select->clone();
    else
        select.reset();

    return *this;
}
}
